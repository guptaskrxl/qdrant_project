import anthropic
import json
import base64
import os
import sys
from pathlib import Path
from typing import Dict, List, Any

from populate_neo4j_latest import populate_from_data as populate_neo4j_from_ocr_data
from populate_qdrant import populate_from_data as populate_qdrant_from_ocr_data

class ProductCatalogExtractor:

    def __init__(self, api_key: str = None):
        """Initialize the extractor with Anthropic API key."""
        self.api_key = api_key or os.environ.get('ANTHROPIC_API_KEY')
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY not found in environment variables")
        
        self.client = anthropic.Anthropic(api_key=self.api_key)
        
    def read_file_as_base64(self, file_path):
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        extension = path.suffix.lower()
        media_type_map = {
            '.pdf': 'application/pdf',
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.webp': 'image/webp',
            '.gif': 'image/gif'
        }
        
        media_type = media_type_map.get(extension)
        if not media_type:
            raise ValueError(f"Unsupported file type: {extension}")
        
        with open(file_path, 'rb') as f:
            file_data = base64.standard_b64encode(f.read()).decode('utf-8')
        
        return file_data, media_type
    
    def extract_products_from_document(self, file_path):

        print(f"Processing document: {file_path}")
        
        file_data, media_type = self.read_file_as_base64(file_path)
        
        prompt = """From the attached machine tools catalog, identify and list all the product names along with description, short_description and attributes if available.  

INSTRUCTIONS:
1. Extract ALL products visible in the catalog
2. For each product, include:
   - A unique product_id (format: PREFIX-XXX where PREFIX is derived from company/category)
   - name: The complete product name including model number
   - description: Detailed information about the product, features, applications, and technical details (2-3 sentences minimum)
   - short_description: A brief one-line summary of what the product is (1 sentence)
   - attributes: A flat dictionary containing ALL attributes at the same level:
     * Extract EVERY attribute mentioned in the document for this product
     * Common attributes include: brand, condition, manufacturer, model, type, category, material, color, weight, voltage, power, speed, frequency, etc.
     * Measurement/dimensional attributes: span, radius, length, width, height, depth, diameter, capacity, load, payload, lifting_height, working_radius, boom_length, rope_length, cable_length, rail_gauge, etc.
     * Only include attributes that are explicitly stated in the document
     * IMPORTANT: Do not limit to the examples above - extract ANY and ALL attributes present for each product

3. Logic for descriptions:
   - short_description: Brief, concise overview (what it is)
   - description: Detailed explanation including features, standards, applications, technical specifications

4. For attributes:
   - Extract EVERY piece of information mentioned about the product as a separate attribute
   - Always include units where applicable (e.g., "5 tons", "15 meters", "220V", "50 Hz")
   - For ranges, include as shown (e.g., "3 mtrs. to 30 mtrs." or "500 kgs. to 20,000 kgs.")
   - Use clear, descriptive attribute names (e.g., "safe_working_load" not just "swl")

Return ONLY a valid JSON object with this structure (this is just an example - include ALL attributes found):
{
  "products": [
    {
      "product_id": "COMP-001",
      "name": "Product Name Model-123",
      "short_description": "Brief one-line description",
      "description": "Detailed multi-sentence description with features and applications",
      "attributes": {
        "brand": "Brand Name",
        "condition": "New",
        "capacity": "5 tons",
        "span": "15 meters",
        "height": "8 meters",
        "voltage": "220V",
        "frequency": "50 Hz",
        "any_other_attribute_found": "value with units"
      }
    }
  ]
}

CRITICAL POINTS:
- All attributes should be at the same level in the attributes dictionary (flat structure)
- Do NOT nest measurements in a separate sub-dictionary
- Extract EVERY attribute mentioned - do not limit to common ones
- If an attribute is mentioned multiple times with different values (like a range), include it appropriately
- Return ONLY the JSON object, no additional text or markdown formatting"""

        # Call Claude API with vision
        try:
            message = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=16000,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "document",
                                "source": {
                                    "type": "base64",
                                    "media_type": media_type,
                                    "data": file_data
                                }
                            },
                            {
                                "type": "text",
                                "text": prompt
                            }
                        ]
                    }
                ]
            )
            
            # Extract response text
            response_text = message.content[0].text
            
            # Parse JSON from response
            # Sometimes Claude wraps JSON in markdown code blocks, so clean it
            response_text = response_text.strip()
            if response_text.startswith('```json'):
                response_text = response_text[7:]
            if response_text.startswith('```'):
                response_text = response_text[3:]
            if response_text.endswith('```'):
                response_text = response_text[:-3]
            response_text = response_text.strip()
            
            extracted_data = json.loads(response_text)
            
            print(f"Successfully extracted {len(extracted_data.get('products', []))} products")
            return extracted_data
            
        except anthropic.APIError as e:
            print(f"API Error: {e}")
            raise
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON response: {e}")
            print(f"Response text: {response_text[:500]}")
            raise
        except Exception as e:
            print(f"Error during extraction: {e}")
            raise
    
    def transform_for_neo4j(self, extracted_data):

        neo4j_products = []
        
        for product in extracted_data.get('products', []):
            neo4j_product = {
                'id': product['product_id'],
                'name': product['name'],
                'short_description': product.get('short_description', '')
            }
            
            attributes_list = []
            attributes = product.get('attributes', {})
            
            for key, value in attributes.items():
                if value:  
                    attributes_list.append({
                        'key': key,
                        'value': str(value)
                    })
            
            neo4j_product['attributes'] = attributes_list
            
            neo4j_products.append(neo4j_product)
        
        return neo4j_products
    
    def transform_for_qdrant(self, extracted_data):

        qdrant_products = []
        
        for product in extracted_data.get('products', []):
            qdrant_product = {
                'id': product['product_id'],
                'name': product['name'],
                'short_description': product.get('short_description', ''),
                'description': product.get('description', '')
            }
            
            qdrant_products.append(qdrant_product)
        
        return qdrant_products
    
    def save_json(self, data, filename):
        """Save data to JSON file."""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Saved data to {filename}")
    
    def populate_databases(self, neo4j_data, qdrant_data):
        
        print("\nPOPULATING NEO4J DATABASE")
        try:
            populate_neo4j_from_ocr_data(neo4j_data)
        except Exception as e:
            print(f"Error populating Neo4j: {e}")
            return False

        print("\nPOPULATING QDRANT DATABASE")
        try:
            populate_qdrant_from_ocr_data(qdrant_data)
        except Exception as e:
            print(f"Error populating Qdrant: {e}")
            return False

        return True
    
    def process_catalog(self, file_path):
        
        print("Step 1: Extracting products from document...")
        extracted_data = self.extract_products_from_document(file_path)
        
        print("\nStep 2: Transforming data for Neo4j")
        neo4j_data = self.transform_for_neo4j(extracted_data)
        self.save_json(neo4j_data, 'final_data_neo4j.json')
        
        print("\nStep 3: Transforming data for Qdrant")
        qdrant_data = self.transform_for_qdrant(extracted_data)
        self.save_json(qdrant_data, 'final_data_qdrant.json')
        
        print("\nStep 4: Populating databases")
        success = self.populate_databases(neo4j_data, qdrant_data)
        
        if success:
            print("\nWORKFLOW COMPLETE")
        else:
            print("\nData files created but database population failed")

def main():

    if len(sys.argv) < 2:
        print("\nExample: python ocr.py catalog.pdf")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    if not Path(file_path).exists():
        print(f"Error: File not found: {file_path}")
        sys.exit(1)
    
    try:
        extractor = ProductCatalogExtractor()
    except ValueError as e:
        print(f"Error: {e}")
        print("\nPlease set ANTHROPIC_API_KEY environment variable:")
        print("  export ANTHROPIC_API_KEY='your-api-key'")
        sys.exit(1)
    
    try:
        extractor.process_catalog(file_path)
    except Exception as e:
        print(f"\nFatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()