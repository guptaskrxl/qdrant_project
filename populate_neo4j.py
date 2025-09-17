# import json
# import os
# import sys
# from neo4j import GraphDatabase
# from typing import Dict, List, Any

# class Neo4jProductLoader:
#     def __init__(self, uri: str, user: str, password: str):
#         self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
#     def close(self):
#         """Close the database connection"""
#         self.driver.close()
        
#     def clear_database(self):
#         """Clear all existing data from the database"""
#         with self.driver.session() as session:
#             session.run("MATCH (n) DETACH DELETE n")
#             print("✓ Database cleared")
            
#     def create_indexes(self):
#         """Create necessary indexes for the database"""
#         with self.driver.session() as session:
#             # Create unique constraint on Product id
#             session.run("""
#                 CREATE CONSTRAINT product_id_unique IF NOT EXISTS
#                 FOR (p:Product) REQUIRE p.id IS UNIQUE
#             """)
            
#             # Create unique constraint on Attribute for uniqueness
#             session.run("""
#                 CREATE CONSTRAINT attribute_unique IF NOT EXISTS
#                 FOR (a:Attribute) REQUIRE (a.key, a.value) IS UNIQUE
#             """)
            
#             # Create full-text search index for fuzzy matching
#             # Drop existing index if it exists (to avoid conflicts)
#             try:
#                 session.run("DROP INDEX product_search IF EXISTS")
#             except:
#                 pass  # Index might not exist
                
#             # Create new full-text index on Product name and short_description
#             session.run("""
#                 CREATE FULLTEXT INDEX product_search IF NOT EXISTS
#                 FOR (p:Product)
#                 ON EACH [p.name, p.short_description]
#             """)
            
#             print("✓ Indexes and constraints created")
            
#     def create_product_node(self, tx, product_data: Dict[str, Any]):
#         """
#         Create a Product node in the database
        
#         Args:
#             tx: Neo4j transaction
#             product_data: Product data dictionary
#         """
#         # Handle empty short_description
#         short_desc = product_data.get('short_description', '')
#         if not short_desc:
#             short_desc = ''
            
#         query = """
#             MERGE (p:Product {id: $id})
#             SET p.name = $name,
#                 p.short_description = $short_description
#             RETURN p
#         """
        
#         tx.run(query, 
#                id=product_data['id'],
#                name=product_data['name'],
#                short_description=short_desc)
        
#     def create_attribute_and_relationship(self, tx, product_id: str, 
#                                          attribute_type: str, 
#                                          attributes: List[Dict[str, str]]):
#         """
#         Create Attribute nodes and relationships to Product
        
#         Args:
#             tx: Neo4j transaction
#             product_id: Product ID
#             attribute_type: Type of attribute (filter, misc, config, key)
#             attributes: List of attribute dictionaries
#         """
#         if not attributes:  # Handle empty attribute lists
#             return
            
#         for attr in attributes:
#             if not attr.get('key') or not attr.get('value'):  # Skip invalid attributes
#                 continue
                
#             query = """
#                 MATCH (p:Product {id: $product_id})
#                 MERGE (a:Attribute {key: $key, value: $value})
#                 SET a.type = $type
#                 MERGE (p)-[:HAS_ATTRIBUTE]->(a)
#             """
            
#             tx.run(query,
#                    product_id=product_id,
#                    key=attr['key'],
#                    value=attr['value'],
#                    type=attribute_type)
    
#     def load_product(self, product_data: Dict[str, Any]):
#         """
#         Load a single product with all its attributes into Neo4j
        
#         Args:
#             product_data: Product data dictionary
#         """
#         with self.driver.session() as session:
#             # Use transaction for atomicity
#             with session.begin_transaction() as tx:
#                 # Create Product node
#                 self.create_product_node(tx, product_data)
                
#                 # Create Attribute nodes and relationships for each attribute type
#                 attribute_types = [
#                     ('filterAttributes', 'filter'),
#                     ('miscAttributes', 'misc'),
#                     ('configAttributes', 'config'),
#                     ('keyAttributes', 'key')
#                 ]
                
#                 for attr_field, attr_type in attribute_types:
#                     attributes = product_data.get(attr_field, [])
#                     self.create_attribute_and_relationship(
#                         tx, product_data['id'], attr_type, attributes
#                     )
                
#                 # Commit transaction
#                 tx.commit()
    
#     def load_products_from_json(self, json_file_path: str):
#         try:
#             with open(json_file_path, 'r', encoding='utf-8') as f:
#                 products = json.load(f)
                
#             print(f"Loading {len(products)} products into Neo4j...")
            
#             # Process products in batches for better performance
#             batch_size = 100
#             for i in range(0, len(products), batch_size):
#                 batch = products[i:i + batch_size]
                
#                 with self.driver.session() as session:
#                     with session.begin_transaction() as tx:
#                         for product in batch:
#                             # Create Product node
#                             self.create_product_node(tx, product)
                            
#                             # Create attributes
#                             attribute_types = [
#                                 ('filterAttributes', 'filter'),
#                                 ('miscAttributes', 'misc'),
#                                 ('configAttributes', 'config'),
#                                 ('keyAttributes', 'key')
#                             ]
                            
#                             for attr_field, attr_type in attribute_types:
#                                 attributes = product.get(attr_field, [])
#                                 self.create_attribute_and_relationship(
#                                     tx, product['id'], attr_type, attributes
#                                 )
                        
#                         tx.commit()
                
#                 # Progress indicator
#                 processed = min(i + batch_size, len(products))
#                 print(f"  Processed {processed}/{len(products)} products...")
            
#             print(f"✓ Successfully loaded {len(products)} products")
            
#         except FileNotFoundError:
#             print(f"Error: File '{json_file_path}' not found")
#             sys.exit(1)
#         except json.JSONDecodeError as e:
#             print(f"Error: Invalid JSON in file '{json_file_path}': {e}")
#             sys.exit(1)
#         except Exception as e:
#             print(f"Error loading products: {e}")
#             sys.exit(1)

# def main():

#     neo4j_uri = os.environ.get('NEO4J_URI', 'bolt://localhost:7687')
#     neo4j_user = os.environ.get('NEO4J_USER', 'neo4j')
#     neo4j_password = os.environ.get('NEO4J_PASSWORD', 'password')
    
#     # JSON file path
#     json_file = 'final_data_neo4j.json'
    
#     print("=" * 50)
#     print("Neo4j Product Data Loader")
#     print("=" * 50)
    
#     # Initialize loader
#     loader = Neo4jProductLoader(neo4j_uri, neo4j_user, neo4j_password)
    
#     try:
#         # Clear existing data
#         print("Clearing existing data...")
#         loader.clear_database()
        
#         # Create indexes
#         print("Creating indexes and constraints...")
#         loader.create_indexes()
        
#         # Load products from JSON
#         print(f"Loading data from '{json_file}'...")
#         loader.load_products_from_json(json_file)
        
#         # Verify data loaded
#         with loader.driver.session() as session:
#             result = session.run("MATCH (p:Product) RETURN count(p) as count")
#             count = result.single()['count']
#             print(f"\n✓ Database now contains {count} products")
            
#             result = session.run("MATCH (a:Attribute) RETURN count(a) as count")
#             attr_count = result.single()['count']
#             print(f"✓ Database now contains {attr_count} unique attributes")
        
#         print("\n✓ Data population completed successfully!")
        
#     except Exception as e:
#         print(f"\nError: {e}")
#         sys.exit(1)
#     finally:
#         loader.close()

# if __name__ == "__main__":
#     main()

##################################################################

import json
import os
import sys
from neo4j import GraphDatabase
from typing import Dict, List, Any


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
                # Get counts for more detailed information
                product_result = session.run("MATCH (p:Product) RETURN count(p) as count")
                product_count = product_result.single()['count']
                
                attr_result = session.run("MATCH (a:Attribute) RETURN count(a) as count")
                attr_count = attr_result.single()['count']
                
                print("\nDatabase contains existing data")
                
                while True:
                    response = input("\nClear the existing database? (y/n): ").strip().lower()
                    
                    if response == 'y' or response == 'yes':
                        session.run("MATCH (n) DETACH DELETE n")
                        print("Database cleared")
                        return True
                    elif response == 'n' or response == 'no':
                        print("Exiting")
                        return False
            
    def create_indexes(self):
        with self.driver.session() as session:
            # Create unique constraint on Product id
            session.run("""
                CREATE CONSTRAINT product_id_unique IF NOT EXISTS
                FOR (p:Product) REQUIRE p.id IS UNIQUE
            """)
            
            # Create unique constraint on Attribute for uniqueness
            session.run("""
                CREATE CONSTRAINT attribute_unique IF NOT EXISTS
                FOR (a:Attribute) REQUIRE (a.key, a.value) IS UNIQUE
            """)
            
            # Create full-text search index for fuzzy matching
            # Drop existing index if it exists (to avoid conflicts)
            try:
                session.run("DROP INDEX product_search IF EXISTS")
            except:
                pass  # Index might not exist
                
            # Create new full-text index on Product name and short_description
            # Simplified version without analyzer options for compatibility
            session.run("""
                CREATE FULLTEXT INDEX product_search IF NOT EXISTS
                FOR (p:Product)
                ON EACH [p.name, p.short_description]
            """)
            
            # Create additional index on name for faster CONTAINS queries
            session.run("""
                CREATE INDEX product_name_index IF NOT EXISTS
                FOR (p:Product) ON (p.name)
            """)
            
            print("Indexes and constraints created")
            
    def create_product_node(self, tx, product_data):

        # Handle empty short_description
        short_desc = product_data.get('short_description', '')
        if not short_desc:
            short_desc = ''
            
        query = """
            MERGE (p:Product {id: $id})
            SET p.name = $name,
                p.short_description = $short_description
            RETURN p
        """
        
        tx.run(query, 
               id=product_data['id'],
               name=product_data['name'],
               short_description=short_desc)
        
    def create_attribute_and_relationship(self, tx, product_id, 
                                         attribute_type, 
                                         attributes):

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
            
            # Process products in batches for better performance
            batch_size = 100
            for i in range(0, len(products), batch_size):
                batch = products[i:i + batch_size]
                
                with self.driver.session() as session:
                    with session.begin_transaction() as tx:
                        for product in batch:
                            # Create Product node
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
                print(f"  Processed {processed}/{len(products)} products...")
            
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
    
    # Initialize loader
    loader = Neo4jProductLoader(neo4j_uri, neo4j_user, neo4j_password)
    
    try:
        if not loader.clear_database_with_confirmation():
            sys.exit(0)
        
        # Create indexes
        print("Creating indexes and constraints")
        loader.create_indexes()
        
        # Load products from JSON
        print(f"Loading data from '{json_file}'")
        loader.load_products_from_json(json_file)
        
        # Verify data loaded
        with loader.driver.session() as session:
            result = session.run("MATCH (p:Product) RETURN count(p) as count")
            count = result.single()['count']
            print(f"\nDatabase now contains {count} products")
        
        print("\nData population complete\n")
        
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)
    finally:
        loader.close()

if __name__ == "__main__":
    main()