import json
import os
import sys
import re
from neo4j import GraphDatabase
from typing import Dict, List, Any, Set


class Neo4jProductLoader:
    
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        self.driver.close()
        
    def check_existing_data(self):
        with self.driver.session() as session:
            result = session.run("MATCH (n) RETURN count(n) as count LIMIT 1")
            count = result.single()['count']
            return count > 0
        
    def clear_database_with_confirmation(self):
        has_data = self.check_existing_data()
        
        if has_data:
            with self.driver.session() as session:
                product_result = session.run("MATCH (p:Product) RETURN count(p) as count")
                product_count = product_result.single()['count']
                
                attr_result = session.run("MATCH (a:Attribute) RETURN count(a) as count")
                attr_count = attr_result.single()['count']
                
                print(f"\nDatabase contains {product_count} products and {attr_count} attributes")
                
                while True:
                    response = input("\nClear the existing database? (y/n): ").strip().lower()
                    
                    if response == 'y' or response == 'yes':
                        session.run("MATCH (n) DETACH DELETE n")
                        print("Database cleared")
                        return True
                    elif response == 'n' or response == 'no':
                        print("Exiting without changes")
                        return False
        else:
            print("Database is empty")
            return True
    
    def extract_product_codes(self, text):
        """Extract product codes from text."""
        if not text:
            return set()
        
        codes = set()
        
        # Alphanumeric with hyphens (e.g., AIUR-06-102J, CX-112)
        pattern1 = r'\b[A-Z0-9]+(?:-[A-Z0-9]+)+\b'
        found_codes = re.findall(pattern1, text.upper())
        codes.update(found_codes)
        
        # Pattern 2: Mixed alphanumeric (at least one letter and one number, 4+ chars)
        pattern2 = r'\b(?=.*[A-Z])(?=.*[0-9])[A-Z0-9]{4,}\b'
        potential_codes = re.findall(pattern2, text.upper())
        codes.update(potential_codes)
        
        return codes
    
    def generate_code_variations(self, code):
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
    
    def extract_search_terms(self, product_data):
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
            
    def create_indexes(self):
        with self.driver.session() as session:
            # Create unique constraint on Product id
            session.run("""
                CREATE CONSTRAINT product_id_unique IF NOT EXISTS
                FOR (p:Product) REQUIRE p.id IS UNIQUE
            """)
            
            # Create unique constraint on Attribute
            session.run("""
                CREATE CONSTRAINT attribute_unique IF NOT EXISTS
                FOR (a:Attribute) REQUIRE (a.key, a.value) IS UNIQUE
            """)
            
            # Drop and recreate full-text search index
            try:
                session.run("DROP INDEX product_search IF EXISTS")
            except:
                pass
                
            # Create full-text index on multiple fields including search_terms
            session.run("""
                CREATE FULLTEXT INDEX product_search IF NOT EXISTS
                FOR (p:Product)
                ON EACH [p.name, p.short_description, p.search_terms]
            """)
            
            # Create index on name for CONTAINS queries
            session.run("""
                CREATE INDEX product_name_index IF NOT EXISTS
                FOR (p:Product) ON (p.name)
            """)
            
            # Create index on search_terms for efficient lookup
            session.run("""
                CREATE INDEX product_search_terms_index IF NOT EXISTS
                FOR (p:Product) ON (p.search_terms)
            """)
            
            # Create indexes on Attribute
            session.run("""
                CREATE INDEX attribute_key_index IF NOT EXISTS
                FOR (a:Attribute) ON (a.key)
            """)
            
            session.run("""
                CREATE INDEX attribute_value_index IF NOT EXISTS
                FOR (a:Attribute) ON (a.value)
            """)
            
            print("Indexes and constraints created")
            
    def create_product_node(self, tx, product_data):
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
        
    def create_attribute_and_relationship(self, tx, product_id, attribute_type, attributes):
        if not attributes: 
            return
            
        for attr in attributes:
            if not attr.get('key') or not attr.get('value'):  
                continue
                
            query = """
                MATCH (p:Product {id: $product_id})
                MERGE (a:Attribute {key: $key, value: $value})
                SET a.type = $type
                MERGE (p)-[:HAS_ATTRIBUTE]->(a)
            """
            
            tx.run(query,
                   product_id=product_id,
                   key=attr['key'],
                   value=attr['value'],
                   type=attribute_type)
    
    def load_products_from_json(self, json_file_path):
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                products = json.load(f)
                
            print(f"Loading {len(products)} products into Neo4j")
            print("Pre-computing search terms and code variations...")
            
            # Process products in batches
            batch_size = 100
            for i in range(0, len(products), batch_size):
                batch = products[i:i + batch_size]
                
                with self.driver.session() as session:
                    with session.begin_transaction() as tx:
                        for product in batch:
                            # Create Product node with search terms
                            self.create_product_node(tx, product)
                            
                            # Create attributes
                            attribute_types = [
                                ('filterAttributes', 'filter'),
                                ('miscAttributes', 'misc'),
                                ('configAttributes', 'config'),
                                ('keyAttributes', 'key')
                            ]
                            
                            for attr_field, attr_type in attribute_types:
                                attributes = product.get(attr_field, [])
                                self.create_attribute_and_relationship(
                                    tx, product['id'], attr_type, attributes
                                )
                        
                        tx.commit()
                
                # Progress indicator
                processed = min(i + batch_size, len(products))
                print(f"  Processed {processed}/{len(products)} products")
            
            print(f"Successfully loaded {len(products)} products with pre-computed search terms")
            
        except FileNotFoundError:
            print(f"Error: File '{json_file_path}' not found")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in file '{json_file_path}': {e}")
            sys.exit(1)
        except Exception as e:
            print(f"Error loading products: {e}")
            sys.exit(1)

def main():
    neo4j_uri = os.environ.get('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_user = os.environ.get('NEO4J_USER', 'neo4j')
    neo4j_password = os.environ.get('NEO4J_PASSWORD', 'password')
    
    json_file = 'final_data_neo4j.json'
    
    loader = Neo4jProductLoader(neo4j_uri, neo4j_user, neo4j_password)
    
    try:
        if not loader.clear_database_with_confirmation():
            sys.exit(0)
        
        print("\nCreating indexes and constraints")
        loader.create_indexes()
        
        print(f"\nLoading data from '{json_file}'")
        loader.load_products_from_json(json_file)
        
        # Verify data loaded
        with loader.driver.session() as session:
            result = session.run("MATCH (p:Product) RETURN count(p) as count")
            count = result.single()['count']
            print(f"\nDatabase now contains {count} products")
        
        print("\nData population complete with optimized search terms!\n")
        
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)
    finally:
        loader.close()

if __name__ == "__main__":
    main()