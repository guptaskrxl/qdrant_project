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
    """
    Extracts product information from PDF/image catalogs using Claude Vision API
    and populates Neo4j and Qdrant databases.
    """
    
    def __init__(self, api_key: str = None):
        """Initialize the extractor with Anthropic API key."""
        self.api_key = api_key or os.environ.get('ANTHROPIC_API_KEY')
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY not found in environment variables")
        
        self.client = anthropic.Anthropic(api_key=self.api_key)
        
    def read_file_as_base64(self, file_path: str) -> tuple:
        """Read file and convert to base64."""
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Determine media type
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
        
        # Read and encode file
        with open(file_path, 'rb') as f:
            file_data = base64.standard_b64encode(f.read()).decode('utf-8')
        
        return file_data, media_type
    
    def extract_products_from_document(self, file_path: str) -> Dict[str, Any]:
        """
        Extract product information from a catalog document using Claude Vision API.
        
        Args:
            file_path: Path to PDF or image file
            
        Returns:
            Dictionary containing extracted products
        """
        print(f"Processing document: {file_path}")
        
        # Read and encode file
        file_data, media_type = self.read_file_as_base64(file_path)
        
        # Prepare the prompt
        prompt = """From the attached machine tools catalog, identify and list all the product names along with description, short_description and attributes if available.  

INSTRUCTIONS:
1. Extract ALL products visible in the catalog
2. For each product, include:
   - A unique product_id (format: PREFIX-XXX where PREFIX is derived from company/category)
   - name: The complete product name including model number
   - description: Detailed information about the product, features, applications, and technical details (2-3 sentences minimum)
   - short_description: A brief one-line summary of what the product is (1 sentence)
   - attributes: A dictionary containing:
     * brand: The manufacturer/brand name
     * condition: "New" (or leave empty if not mentioned)
     * measurements: A nested dictionary with relevant dimensions like:
       - span, radius, length, width, height, capacity, load, etc. (include units)
       - Only include measurements that are explicitly stated in the document

3. Logic for descriptions:
   - short_description: Brief, concise overview (what it is)
   - description: Detailed explanation including features, standards, applications, technical specifications

4. For measurements, extract ALL dimensional data available including:
   - Physical dimensions (span, radius, height, etc.)
   - Capacity/Load specifications (safe working load, payload, etc.)
   - Ranges (e.g., "3 mtrs. to 30 mtrs." or "500 kgs. to 20,000 kgs.")

Return ONLY a valid JSON object with this structure:
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
        "measurements": {
          "dimension_name": "value with units",
          "capacity": "value with units"
        }
      }
    }
  ]
}

IMPORTANT: Return ONLY the JSON object, no additional text or markdown formatting."""

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
    
    def transform_for_neo4j(self, extracted_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Transform extracted data to Neo4j format.
        Neo4j expects: id, name, short_description, and separate attribute arrays.
        """
        neo4j_products = []
        
        for product in extracted_data.get('products', []):
            neo4j_product = {
                'id': product['product_id'],
                'name': product['name'],
                'short_description': product.get('short_description', '')
            }
            
            # Transform attributes into separate arrays by type
            attributes = product.get('attributes', {})
            
            # Filter attributes: brand, condition, measurements
            filter_attrs = []
            
            if 'brand' in attributes and attributes['brand']:
                filter_attrs.append({'key': 'brand', 'value': attributes['brand']})
            
            if 'condition' in attributes and attributes['condition']:
                filter_attrs.append({'key': 'condition', 'value': attributes['condition']})
            
            # Handle measurements
            measurements = attributes.get('measurements', {})
            if measurements:
                for key, value in measurements.items():
                    if value:
                        filter_attrs.append({'key': key, 'value': str(value)})
            
            neo4j_product['filterAttributes'] = filter_attrs
            neo4j_product['miscAttributes'] = []
            neo4j_product['configAttributes'] = []
            neo4j_product['keyAttributes'] = []
            
            neo4j_products.append(neo4j_product)
        
        return neo4j_products
    
    def transform_for_qdrant(self, extracted_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Transform extracted data to Qdrant format.
        Qdrant expects: id, name, short_description, description.
        """
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
    
    def save_json(self, data: Any, filename: str):
        """Save data to JSON file."""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Saved data to {filename}")
    
    def populate_databases(self, neo4j_data, qdrant_data):
        print("\n" + "="*60)
        print("POPULATING NEO4J DATABASE")
        print("="*60)
        try:
            populate_neo4j_from_ocr_data(neo4j_data)
        except Exception as e:
            print(f"Error populating Neo4j: {e}")
            return False

        print("\n" + "="*60)
        print("POPULATING QDRANT DATABASE")
        print("="*60)
        try:
            populate_qdrant_from_ocr_data(qdrant_data)
        except Exception as e:
            print(f"Error populating Qdrant: {e}")
            return False

        return True

    
    def process_catalog(self, file_path: str, populate_dbs: bool = True):
        """
        Complete workflow: Extract from catalog and populate databases.
        
        Args:
            file_path: Path to catalog PDF or image
            populate_dbs: Whether to automatically populate databases (default: True)
        """
        print("\n" + "="*60)
        print("PRODUCT CATALOG EXTRACTION WORKFLOW")
        print("="*60 + "\n")
        
        # Step 1: Extract products from document
        print("Step 1: Extracting products from document...")
        extracted_data = self.extract_products_from_document(file_path)
        
        # Step 2: Transform and save for Neo4j
        print("\nStep 2: Transforming data for Neo4j...")
        neo4j_data = self.transform_for_neo4j(extracted_data)
        self.save_json(neo4j_data, 'final_data_neo4j.json')
        
        # Step 3: Transform and save for Qdrant
        print("\nStep 3: Transforming data for Qdrant...")
        qdrant_data = self.transform_for_qdrant(extracted_data)
        self.save_json(qdrant_data, 'final_data_qdrant.json')
        
        # Step 4: Populate databases
        if populate_dbs:
            print("\nStep 4: Populating databases...")
            success = self.populate_databases(neo4j_data, qdrant_data)
            
            if success:
                print("\n" + "="*60)
                print("WORKFLOW COMPLETED SUCCESSFULLY!")
                print("="*60)
            else:
                print("\n" + "="*60)
                print("WORKFLOW COMPLETED WITH ERRORS")
                print("Data files created but database population failed")
                print("="*60)
        else:
            print("\nSkipping database population (populate_dbs=False)")
            print("\n" + "="*60)
            print("EXTRACTION COMPLETED!")
            print("JSON files created. Run populate scripts manually.")
            print("="*60)


def main():

    if len(sys.argv) < 2:
        print("Usage: python ocr_extraction_workflow.py <path_to_catalog_file> [--no-populate]")
        print("\nExample:")
        print("  python ocr_extraction_workflow.py catalog.pdf")
        print("  python ocr_extraction_workflow.py catalog.pdf --no-populate")
        sys.exit(1)
    
    file_path = sys.argv[1]
    populate_dbs = '--no-populate' not in sys.argv
    
    # Check if file exists
    if not Path(file_path).exists():
        print(f"Error: File not found: {file_path}")
        sys.exit(1)
    
    # Initialize extractor
    try:
        extractor = ProductCatalogExtractor()
    except ValueError as e:
        print(f"Error: {e}")
        print("\nPlease set ANTHROPIC_API_KEY environment variable:")
        print("  export ANTHROPIC_API_KEY='your-api-key'")
        sys.exit(1)
    
    # Process catalog
    try:
        extractor.process_catalog(file_path, populate_dbs=populate_dbs)
    except Exception as e:
        print(f"\nFatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()