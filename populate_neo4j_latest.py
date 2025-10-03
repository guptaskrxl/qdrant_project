import json
import os
import sys
import re
from neo4j import GraphDatabase
from typing import Dict, List, Any, Set

class Neo4jProductLoader:
    
    def __init__(self, uri, user, password, auto_create_schema = True):

        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
        if auto_create_schema:
            print("Initializing Neo4j schema and indexes")
            self._create_schema_and_indexes()
            print("Schema and indexes ready")
        
    def close(self):
        self.driver.close()
        
    def __enter__(self):
        return self
    
    def _create_schema_and_indexes(self):
        with self.driver.session() as session:
            session.run("""
                CREATE CONSTRAINT product_id_unique IF NOT EXISTS
                FOR (p:Product) REQUIRE p.id IS UNIQUE
            """)
            
            session.run("""
                CREATE CONSTRAINT attribute_unique IF NOT EXISTS
                FOR (a:Attribute) REQUIRE (a.key, a.value) IS UNIQUE
            """)
            
            try:
                session.run("DROP INDEX product_search IF EXISTS")
            except:
                pass
                
            session.run("""
                CREATE FULLTEXT INDEX product_search IF NOT EXISTS
                FOR (p:Product)
                ON EACH [p.name, p.short_description, p.search_terms]
            """)
            
            session.run("""
                CREATE INDEX product_name_index IF NOT EXISTS
                FOR (p:Product) ON (p.name)
            """)
            
            session.run("""
                CREATE INDEX product_search_terms_index IF NOT EXISTS
                FOR (p:Product) ON (p.search_terms)
            """)
            
            session.run("""
                CREATE INDEX attribute_key_index IF NOT EXISTS
                FOR (a:Attribute) ON (a.key)
            """)
            
            session.run("""
                CREATE INDEX attribute_value_index IF NOT EXISTS
                FOR (a:Attribute) ON (a.value)
            """)
    
    def check_existing_data(self) -> bool:
        """Check if database contains any data."""
        with self.driver.session() as session:
            result = session.run("MATCH (n) RETURN count(n) as count LIMIT 1")
            count = result.single()['count']
            return count > 0
    
    def get_database_stats(self) -> Dict[str, int]:
        """Get current database statistics."""
        with self.driver.session() as session:
            product_result = session.run("MATCH (p:Product) RETURN count(p) as count")
            product_count = product_result.single()['count']
            
            attr_result = session.run("MATCH (a:Attribute) RETURN count(a) as count")
            attr_count = attr_result.single()['count']
            
            return {
                'products': product_count,
                'attributes': attr_count
            }
    
    def clear_database(self, confirm: bool = True) -> bool:

        has_data = self.check_existing_data()
        
        if not has_data:
            print("Database is empty")
            return True
        
        stats = self.get_database_stats()
        print(f"\nDatabase contains {stats['products']} products and {stats['attributes']} attributes")
        
        if confirm:
            while True:
                response = input("\nClear the existing database? (y/n): ").strip().lower()
                
                if response in ['y', 'yes']:
                    break
                elif response in ['n', 'no']:
                    print("Exiting without changes")
                    return False
                else:
                    print("Please enter 'y' or 'n'")
        
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        
        print("Database cleared")
        return True
    
    # ========================================================================
    # SEARCH TERM EXTRACTION
    # ========================================================================
    
    def extract_product_codes(self, text: str) -> Set[str]:
        """Extract product codes from text."""
        if not text:
            return set()
        
        codes = set()
        
        # Pattern 1: Alphanumeric with hyphens (e.g., AIUR-06-102J, CX-112)
        pattern1 = r'\b[A-Z0-9]+(?:-[A-Z0-9]+)+\b'
        found_codes = re.findall(pattern1, text.upper())
        codes.update(found_codes)
        
        # Pattern 2: Mixed alphanumeric (at least one letter and one number, 4+ chars)
        pattern2 = r'\b(?=.*[A-Z])(?=.*[0-9])[A-Z0-9]{4,}\b'
        potential_codes = re.findall(pattern2, text.upper())
        codes.update(potential_codes)
        
        return codes
    
    def generate_code_variations(self, code: str) -> Set[str]:
        """Generate variations of a product code for better search matching."""
        variations = {code}  # Original
        
        # Version without hyphens
        no_hyphen = code.replace('-', '')
        if no_hyphen != code:
            variations.add(no_hyphen)
        
        # Version with spaces instead of hyphens
        space_version = code.replace('-', ' ')
        if space_version != code:
            variations.add(space_version)
        
        # Lowercase versions of all
        variations.update({v.lower() for v in list(variations)})
        
        return variations
    
    def extract_search_terms(self, product_data: Dict[str, Any]) -> Set[str]:
        """Extract all searchable terms from a product including code variations."""
        search_terms = set()
        
        # Extract from name
        if product_data.get('name'):
            name = product_data['name']
            # Add the full name
            search_terms.add(name.lower())
            
            # Extract and add product codes with variations
            codes = self.extract_product_codes(name)
            for code in codes:
                search_terms.update(self.generate_code_variations(code))
            
            # Add individual words (excluding codes)
            # First remove codes from name to avoid duplication
            name_without_codes = name
            for code in codes:
                for variation in self.generate_code_variations(code):
                    name_without_codes = re.sub(r'\b' + re.escape(variation) + r'\b', '', 
                                               name_without_codes, flags=re.IGNORECASE)
            
            # Now add remaining words
            words = re.split(r'[\s\-_,;:.()]+', name_without_codes)
            for word in words:
                word = word.strip().lower()
                if len(word) > 1:  # Skip single chars
                    search_terms.add(word)
        
        # Extract from short_description
        if product_data.get('short_description'):
            desc = product_data['short_description']
            
            # Extract codes from description
            codes = self.extract_product_codes(desc)
            for code in codes:
                search_terms.update(self.generate_code_variations(code))
            
            # Add important words from description (limit to avoid bloat)
            desc_words = re.split(r'[\s\-_,;:.()]+', desc)[:20]  # First 20 words
            for word in desc_words:
                word = word.strip().lower()
                if len(word) > 2:  # Slightly longer threshold for description
                    search_terms.add(word)
        
        return search_terms
    
    # ========================================================================
    # NODE AND RELATIONSHIP CREATION
    # ========================================================================
    
    def _create_product_node(self, tx, product_data: Dict[str, Any]):
        """Create a Product node with pre-computed search terms."""
        # Extract search terms
        search_terms = self.extract_search_terms(product_data)
        
        # Convert search terms set to space-separated string for full-text indexing
        search_terms_str = ' '.join(search_terms)
        
        # Handle empty short_description
        short_desc = product_data.get('short_description', '')
        if not short_desc:
            short_desc = ''
            
        query = """
            MERGE (p:Product {id: $id})
            SET p.name = $name,
                p.short_description = $short_description,
                p.search_terms = $search_terms,
                p.search_terms_list = $search_terms_list
            RETURN p
        """
        
        tx.run(query, 
               id=product_data['id'],
               name=product_data['name'],
               short_description=short_desc,
               search_terms=search_terms_str,  # String for full-text search
               search_terms_list=list(search_terms))  # List for exact matching
        
    def _create_attributes_and_relationships(self, tx, product_id: str, 
                                            attributes: List[Dict[str, str]]):
        """
        Create attribute nodes and relationships to products.
        This method handles ANY attributes - no type restrictions.
        """
        if not attributes: 
            return
            
        for attr in attributes:
            if not attr.get('key') or not attr.get('value'):  
                continue
                
            query = """
                MATCH (p:Product {id: $product_id})
                MERGE (a:Attribute {key: $key, value: $value})
                MERGE (p)-[:HAS_ATTRIBUTE]->(a)
            """
            
            tx.run(query,
                   product_id=product_id,
                   key=attr['key'],
                   value=attr['value'])
    
    # ========================================================================
    # DATA LOADING
    # ========================================================================
    
    def _load_products(self, products: List[Dict[str, Any]], batch_size: int = 100):
        """
        Core method to load products into Neo4j.
        
        Args:
            products: List of product dictionaries
            batch_size: Number of products to process per batch
        """
        print(f"Loading {len(products)} products into Neo4j")
        print("Pre-computing search terms and code variations...")
        
        # Process products in batches
        for i in range(0, len(products), batch_size):
            batch = products[i:i + batch_size]
            
            with self.driver.session() as session:
                with session.begin_transaction() as tx:
                    for product in batch:
                        # Create Product node with search terms
                        self._create_product_node(tx, product)
                        
                        # Create attributes (single array - handles ANY attributes)
                        attributes = product.get('attributes', [])
                        self._create_attributes_and_relationships(
                            tx, product['id'], attributes
                        )
                    
                    tx.commit()
            
            # Progress indicator
            processed = min(i + batch_size, len(products))
            print(f"  Processed {processed}/{len(products)} products")
        
        print(f"Successfully loaded {len(products)} products with pre-computed search terms")
    
    def load_products_from_json(self, json_file_path: str):
        """
        Load products from a JSON file.
        
        Args:
            json_file_path: Path to the JSON file containing products
        """
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                products = json.load(f)
            
            self._load_products(products)
            
        except FileNotFoundError:
            print(f"Error: File '{json_file_path}' not found")
            raise
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in file '{json_file_path}': {e}")
            raise
        except Exception as e:
            print(f"Error loading products: {e}")
            raise
    
    def load_products_from_data(self, products: List[Dict[str, Any]]):
        """
        Load products from a list of dictionaries.
        
        Args:
            products: List of product dictionaries
        """
        self._load_products(products)


# ============================================================================
# MODULE-LEVEL FUNCTIONS (for external usage like ocr.py)
# ============================================================================

def populate_from_data(products: List[Dict[str, Any]], clear_first: bool = False):

    neo4j_uri = os.environ.get('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_user = os.environ.get('NEO4J_USER', 'neo4j')
    neo4j_password = os.environ.get('NEO4J_PASSWORD', 'password')
    
    with Neo4jProductLoader(neo4j_uri, neo4j_user, neo4j_password) as loader:
        if clear_first:
            loader.clear_database(confirm=False)
        
        loader.load_products_from_data(products)
        
        # Verify and report
        stats = loader.get_database_stats()
        print(f"Database now contains {stats['products']} products and {stats['attributes']} attributes")


def main():
    """Main function for standalone execution."""
    neo4j_uri = os.environ.get('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_user = os.environ.get('NEO4J_USER', 'neo4j')
    neo4j_password = os.environ.get('NEO4J_PASSWORD', 'password')
    
    json_file = 'final_data_neo4j.json'
    
    with Neo4jProductLoader(neo4j_uri, neo4j_user, neo4j_password) as loader:
        # Clear database with user confirmation
        if not loader.clear_database(confirm=True):
            sys.exit(0)
        
        # Load data from JSON file
        print(f"\nLoading data from '{json_file}'")
        try:
            loader.load_products_from_json(json_file)
        except Exception as e:
            print(f"\nError: {e}")
            sys.exit(1)
        
        # Show final statistics
        stats = loader.get_database_stats()
        print(f"\nDatabase now contains {stats['products']} products and {stats['attributes']} attributes")
        print("\nData population complete with optimized search terms!\n")


if __name__ == "__main__":
    main()