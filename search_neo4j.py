# #!/usr/bin/env python3
# """
# search_app.py - Interactive CLI for searching products in Qdrant vector database
# """

# import sys
# from typing import List, Optional
# from sentence_transformers import SentenceTransformer
# from qdrant_client import QdrantClient


# class ProductSearchEngine:
#     """Product search engine using Qdrant vector database."""
    
#     def __init__(self, host: str = "localhost", port: int = 6334):
#         """
#         Initialize the search engine.
        
#         Args:
#             host: Qdrant server host
#             port: Qdrant server port
#         """
#         self.collection_name = "products"
#         self.model_name = "all-MiniLM-L6-v2"
        
#         # Initialize sentence transformer
#         print("Initializing search engine...")
#         print(f"Loading model '{self.model_name}'...")
#         self.model = SentenceTransformer(self.model_name)
#         print("✓ Model loaded successfully")
        
#         # Initialize Qdrant client
#         try:
#             self.client = QdrantClient(
#                 host=host,
#                 port=port,
#                 timeout=30
#             )
#             # Test connection and collection
#             collection_info = self.client.get_collection(self.collection_name)
#             print(f"✓ Connected to Qdrant at http://{host}:{port}")
#             print(f"✓ Collection '{self.collection_name}' found with {collection_info.vectors_count} vectors")
#         except Exception as e:
#             print(f"✗ Failed to connect to Qdrant or access collection: {e}")
#             print("\nPlease ensure:")
#             print("1. Qdrant is running (docker-compose up -d)")
#             print("2. The database has been populated (python populate_qdrant.py)")
#             sys.exit(1)
    
#     def search(self, query: str, limit: int = 10) -> List[dict]:
#         """
#         Search for products similar to the query.
        
#         Args:
#             query: Search query string
#             limit: Maximum number of results to return
            
#         Returns:
#             List of search results with product information
#         """
#         # Generate query embedding
#         query_vector = self.model.encode(query).tolist()
        
#         # Perform search
#         try:
#             results = self.client.search(
#                 collection_name=self.collection_name,
#                 query_vector=query_vector,
#                 limit=limit,
#                 with_payload=True
#             )
            
#             return results
#         except Exception as e:
#             print(f"✗ Search error: {e}")
#             return []
    
#     def format_results(self, results: List) -> str:
#         """
#         Format search results for display.
        
#         Args:
#             results: List of search results
            
#         Returns:
#             Formatted string of results
#         """
#         if not results:
#             return "No matching products found."
        
#         output = ["\nTop 10 most similar product IDs:"]
#         output.append("-" * 40)
        
#         for i, result in enumerate(results, 1):
#             product_id = result.payload.get('product_id', 'Unknown')
#             score = result.score
            
#             # Include product name if available
#             name = result.payload.get('name', '')
#             if name:
#                 output.append(f"{i:2}. ID: {product_id} (Score: {score:.4f})")
#                 output.append(f"    Name: {name}")
#             else:
#                 output.append(f"{i:2}. ID: {product_id} (Score: {score:.4f})")
        
#         output.append("-" * 40)
#         return "\n".join(output)


# def print_welcome():
#     """Print welcome message and instructions."""
#     print("\n" + "=" * 60)
#     print("Product Search System - Interactive CLI")
#     print("=" * 60)
#     print("\nInstructions:")
#     print("- Enter a product query to search for similar products")
#     print("- Type 'exit' to quit the application")
#     print("- Type 'help' for additional commands")
#     print("\n" + "=" * 60)


# def print_help():
#     """Print help information."""
#     print("\nAvailable commands:")
#     print("  exit    - Quit the application")
#     print("  help    - Show this help message")
#     print("  clear   - Clear the screen")
#     print("  stats   - Show database statistics")
#     print("\nSearch tips:")
#     print("  - Use descriptive keywords")
#     print("  - Combine multiple attributes (e.g., 'blue wireless headphones')")
#     print("  - Try different phrasings for better results")


# def clear_screen():
#     """Clear the terminal screen."""
#     import os
#     os.system('cls' if os.name == 'nt' else 'clear')


# def show_stats(engine: ProductSearchEngine):
#     """Show database statistics."""
#     try:
#         info = engine.client.get_collection(engine.collection_name)
#         print(f"\nDatabase Statistics:")
#         print(f"  Collection: {engine.collection_name}")
#         print(f"  Total vectors: {info.vectors_count}")
#         print(f"  Status: {info.status}")
#         print(f"  Vector size: {info.config.params.vectors.size}")
#         print(f"  Distance metric: {info.config.params.vectors.distance}")
#     except Exception as e:
#         print(f"✗ Error fetching statistics: {e}")


# def main():
#     """Main execution function."""
#     # Print welcome message
#     print_welcome()
    
#     # Initialize search engine
#     try:
#         engine = ProductSearchEngine()
#     except Exception as e:
#         print(f"✗ Failed to initialize search engine: {e}")
#         sys.exit(1)
    
#     print("\nReady for searches!\n")
    
#     # Interactive search loop
#     while True:
#         try:
#             # Get user input
#             query = input("Enter a product query (or type 'exit' to quit): ").strip()
            
#             # Check for special commands
#             if query.lower() == 'exit':
#                 print("\nThank you for using the Product Search System!")
#                 print("Goodbye!")
#                 break
            
#             elif query.lower() == 'help':
#                 print_help()
#                 continue
            
#             elif query.lower() == 'clear':
#                 clear_screen()
#                 print_welcome()
#                 print("\nReady for searches!\n")
#                 continue
            
#             elif query.lower() == 'stats':
#                 show_stats(engine)
#                 continue
            
#             elif not query:
#                 print("Please enter a search query or type 'help' for commands.")
#                 continue
            
#             # Perform search
#             print(f"\nSearching for: '{query}'...")
#             results = engine.search(query, limit=10)
            
#             # Display results
#             formatted_results = engine.format_results(results)
#             print(formatted_results)
#             print()
            
#         except KeyboardInterrupt:
#             print("\n\nInterrupted by user. Exiting...")
#             break
#         except Exception as e:
#             print(f"✗ An error occurred: {e}")
#             print("Please try again or type 'exit' to quit.")


# if __name__ == "__main__":
#     main()

####################################################################
# below code works for partials very well and handles model name without dash
####################################################################

# #!/usr/bin/env python3
# """
# search_system.py - Interactive command-line product search system using Neo4j
# """

# import os
# import sys
# from neo4j import GraphDatabase
# from typing import List, Dict


# class ProductSearchSystem:
#     """Class to handle product search functionality using Neo4j"""
    
#     def __init__(self, uri: str, user: str, password: str):
#         """
#         Initialize connection to Neo4j database
        
#         Args:
#             uri: Neo4j connection URI
#             user: Neo4j username
#             password: Neo4j password
#         """
#         self.driver = GraphDatabase.driver(uri, auth=(user, password))
#         self.verify_connection()
        
#     def close(self):
#         """Close the database connection"""
#         self.driver.close()
        
#     def verify_connection(self):
#         """Verify database connection and that data exists"""
#         try:
#             with self.driver.session() as session:
#                 # Check if products exist
#                 result = session.run("MATCH (p:Product) RETURN count(p) as count")
#                 count = result.single()['count']
                
#                 if count == 0:
#                     print("Warning: No products found in database.")
#                     print("Please run 'populate_neo4j.py' first to load product data.")
#                     sys.exit(1)
#                 else:
#                     print(f"✓ Connected to Neo4j. Found {count} products in database.")
                
#                 # Check if full-text index exists
#                 result = session.run("SHOW INDEXES")
#                 indexes = list(result)
#                 fulltext_exists = any('product_search' in str(idx) for idx in indexes)
                
#                 if not fulltext_exists:
#                     print("Warning: Full-text search index not found. Creating it now...")
#                     self.create_search_index(session)
                    
#         except Exception as e:
#             print(f"Error connecting to Neo4j: {e}")
#             print("Please ensure Neo4j is running and accessible.")
#             sys.exit(1)
    
#     def create_search_index(self, session):
#         """Create the full-text search index if it doesn't exist"""
#         try:
#             # Drop existing index if it exists
#             session.run("DROP INDEX product_search IF EXISTS")
#         except:
#             pass
        
#         # Create new full-text index
#         session.run("""
#             CREATE FULLTEXT INDEX product_search 
#             FOR (p:Product)
#             ON EACH [p.name, p.short_description]
#         """)
#         print("✓ Full-text search index created")
    
#     def search_products(self, query_string: str, limit: int = 10) -> List[Dict]:
#         """
#         Search for products using multiple strategies
        
#         Args:
#             query_string: User's search query
#             limit: Maximum number of results to return (default: 10)
            
#         Returns:
#             List of product dictionaries
#         """
#         if not query_string.strip():
#             return []
        
#         with self.driver.session() as session:
#             query_cleaned = query_string.strip()
            
#             # First, try full-text search
#             products = self._search_fulltext(session, query_cleaned, limit)
            
#             # If no results, try CONTAINS search as fallback
#             if not products:
#                 products = self._search_contains(session, query_cleaned, limit)
            
#             # If still no results, try breaking up the query
#             if not products and len(query_cleaned.split()) > 1:
#                 products = self._search_individual_terms(session, query_cleaned, limit)
            
#             return products
    
#     def _search_fulltext(self, session, query: str, limit: int) -> List[Dict]:
#         """
#         Search using Neo4j full-text index
#         """
#         products = []
        
#         # Try different query formats
#         query_variations = [
#             query,  # Original query
#             f'"{query}"',  # Exact phrase
#             ' OR '.join(query.split()),  # OR between words
#             ' AND '.join(query.split()),  # AND between words
#             ' '.join([f'{word}~' for word in query.split()]),  # Fuzzy for each word
#             ' '.join([f'{word}~2' for word in query.split()]),  # More fuzzy
#         ]
        
#         for query_variant in query_variations:
#             if products:  # Stop if we found results
#                 break
                
#             try:
#                 cypher_query = """
#                     CALL db.index.fulltext.queryNodes('product_search', $query)
#                     YIELD node, score
#                     RETURN node.id as product_id, 
#                            node.name as product_name,
#                            node.short_description as description,
#                            score
#                     ORDER BY score DESC
#                     LIMIT $limit
#                 """
                
#                 result = session.run(cypher_query, query=query_variant, limit=limit)
                
#                 for record in result:
#                     product = {
#                         'id': record['product_id'],
#                         'name': record['product_name'],
#                         'description': record['description'] or "",
#                         'score': record['score']
#                     }
                    
#                     # Avoid duplicates
#                     if not any(p['id'] == product['id'] for p in products):
#                         products.append(product)
                        
#             except Exception as e:
#                 # Continue to next variation if this one fails
#                 continue
        
#         return products[:limit]
    
#     def _search_contains(self, session, query: str, limit: int) -> List[Dict]:
#         """
#         Fallback search using CONTAINS for partial matches
#         """
#         # Try both case-sensitive and case-insensitive
#         cypher_query = """
#             MATCH (p:Product)
#             WHERE toLower(p.name) CONTAINS toLower($search_term)
#                OR (p.short_description IS NOT NULL AND toLower(p.short_description) CONTAINS toLower($search_term))
#             WITH p, 
#                  CASE 
#                     WHEN toLower(p.name) CONTAINS toLower($search_term) THEN 2.0
#                     ELSE 1.0
#                  END as score
#             RETURN p.id as product_id,
#                    p.name as product_name,
#                    p.short_description as description,
#                    score
#             ORDER BY score DESC, p.name
#             LIMIT $limit
#         """
        
#         try:
#             result = session.run(cypher_query, search_term=query, limit=limit)
#             products = []
            
#             for record in result:
#                 products.append({
#                     'id': record['product_id'],
#                     'name': record['product_name'],
#                     'description': record['description'] or "",
#                     'score': record['score']
#                 })
            
#             return products
#         except Exception as e:
#             print(f"Contains search error: {e}")
#             return []
    
#     def _search_individual_terms(self, session, query: str, limit: int) -> List[Dict]:
#         """
#         Search for products matching any individual term
#         """
#         words = query.split()
        
#         # Build a query that looks for any word
#         where_clauses = []
#         for word in words:
#             where_clauses.append(f"toLower(p.name) CONTAINS toLower('{word}')")
#             where_clauses.append(f"(p.short_description IS NOT NULL AND toLower(p.short_description) CONTAINS toLower('{word}'))")
        
#         where_condition = " OR ".join(where_clauses)
        
#         cypher_query = f"""
#             MATCH (p:Product)
#             WHERE {where_condition}
#             WITH p, 
#                  size([word IN {words} WHERE toLower(p.name) CONTAINS toLower(word)]) as name_matches,
#                  size([word IN {words} WHERE p.short_description IS NOT NULL AND toLower(p.short_description) CONTAINS toLower(word)]) as desc_matches
#             RETURN p.id as product_id,
#                    p.name as product_name,
#                    p.short_description as description,
#                    (name_matches * 2.0 + desc_matches) as score
#             ORDER BY score DESC, p.name
#             LIMIT {limit}
#         """
        
#         try:
#             result = session.run(cypher_query)
#             products = []
            
#             for record in result:
#                 products.append({
#                     'id': record['product_id'],
#                     'name': record['product_name'],
#                     'description': record['description'] or "",
#                     'score': record['score']
#                 })
            
#             return products
#         except Exception as e:
#             print(f"Individual terms search error: {e}")
#             return []
    
#     def format_search_results(self, products: List[dict]) -> str:
#         """
#         Format search results for display
        
#         Args:
#             products: List of product dictionaries
            
#         Returns:
#             Formatted string for display
#         """
#         if not products:
#             return "No products found."
        
#         output = ["\n" + "=" * 80]
#         output.append(f"Found {len(products)} product(s):")
#         output.append("=" * 80)
        
#         for i, product in enumerate(products, 1):
#             output.append(f"\n{i}. Product ID: {product['id']}")
#             output.append(f"   Name: {product['name']}")
            
#             desc = product['description']
#             if desc and len(desc) > 100:
#                 desc = desc[:97] + "..."
#             if desc:
#                 output.append(f"   Description: {desc}")
                
#             output.append(f"   Relevance Score: {product['score']:.3f}")
        
#         output.append("=" * 80)
        
#         return "\n".join(output)
    
#     def run_interactive_search(self):
#         """Run the interactive command-line search interface"""
#         print("\n" + "=" * 80)
#         print("Product Search System")
#         print("=" * 80)
#         print("Enter your search query to find products.")
#         print("Type 'exit' to quit the program.")
#         print("=" * 80)
        
#         while True:
#             try:
#                 # Get user input
#                 print("\n")
#                 query = input("Enter search query: ").strip()
                
#                 # Check for exit command
#                 if query.lower() == 'exit':
#                     print("\nExiting search system. Goodbye!")
#                     break
                
#                 # Skip empty queries
#                 if not query:
#                     print("Please enter a search query.")
#                     continue
                
#                 # Perform search
#                 print(f"\nSearching for: '{query}'...")
#                 products = self.search_products(query)
                
#                 # Display results
#                 if products:
#                     # Print just the top 10 product IDs as requested
#                     print("\nTop 10 Product IDs:")
#                     print("-" * 40)
#                     for i, product in enumerate(products[:10], 1):
#                         print(f"{i:2}. {product['id']}")
                    
#                     # Also show detailed results for better user experience
#                     print(self.format_search_results(products))
#                 else:
#                     print("No products found.")
                    
#                     # Provide debugging info
#                     print("\nTroubleshooting tips:")
#                     print("1. Check if the product exists in the database")
#                     print("2. Try searching with fewer words")
#                     print("3. Try searching with just the product code (e.g., 'CX-112')")
                
#             except KeyboardInterrupt:
#                 print("\n\nInterrupted by user. Exiting...")
#                 break
#             except EOFError:
#                 print("\n\nEnd of input. Exiting...")
#                 break
#             except Exception as e:
#                 print(f"Error during search: {e}")
#                 print("Please try again.")


# def main():
#     """Main function to run the search system"""
    
#     # Get Neo4j connection details from environment variables
#     # Default to localhost for running outside Docker, or neo4j hostname for inside Docker
#     neo4j_uri = os.environ.get('NEO4J_URI', 'bolt://localhost:7687')
#     neo4j_user = os.environ.get('NEO4J_USER', 'neo4j')
#     neo4j_password = os.environ.get('NEO4J_PASSWORD', 'password')
    
#     print(f"Connecting to Neo4j at {neo4j_uri}...")
    
#     # Initialize search system
#     search_system = ProductSearchSystem(neo4j_uri, neo4j_user, neo4j_password)
    
#     try:
#         # Run interactive search
#         search_system.run_interactive_search()
#     finally:
#         search_system.close()


# if __name__ == "__main__":
#     main()

################################################################
# below is the attribute search code
################################################################

import os
import sys
from neo4j import GraphDatabase
from typing import List, Dict

class ProductSearchSystem:

    def __init__(self, uri, user, password):

        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.verify_connection()
        
    def close(self):
        self.driver.close()
        
    def verify_connection(self):
        try:
            with self.driver.session() as session:
                # Check if products exist
                result = session.run("MATCH (p:Product) RETURN count(p) as count")
                count = result.single()['count']
                
                if count == 0:
                    print("Warning: No products found in database.")
                    sys.exit(1)
                else:
                    print(f"Connected to Neo4j. Found {count} products in database.")
                
                # Check if attributes exist
                result = session.run("MATCH (a:Attribute) RETURN count(a) as count")
                attr_count = result.single()['count']
                print(f"Found {attr_count} unique attributes in database.")
                
                # Check if full-text index exists
                result = session.run("SHOW INDEXES")
                indexes = list(result)
                fulltext_exists = any('product_search' in str(idx) for idx in indexes)
                
                if not fulltext_exists:
                    print("Warning: Full-text search index not found. Creating it now...")
                    self.create_search_index(session)
                    
        except Exception as e:
            print(f"Error connecting to Neo4j: {e}")
            sys.exit(1)
    
    def create_search_index(self, session):
        """Create the full-text search index if it doesn't exist"""
        try:
            # Drop existing index if it exists
            session.run("DROP INDEX product_search IF EXISTS")
        except:
            pass
        
        # Create new full-text index
        session.run("""
            CREATE FULLTEXT INDEX product_search 
            FOR (p:Product)
            ON EACH [p.name, p.short_description]
        """)
        print("Full-text search index created")
    
    def search_products(self, query_string, limit=10):

        if not query_string.strip():
            return []
        
        with self.driver.session() as session:
            query_cleaned = query_string.strip()
            
            # Combine results from different search strategies
            all_results = {}
            
            # Strategy 1: Full-text search on name and description
            fulltext_results = self._search_fulltext(session, query_cleaned, limit * 2)
            for product in fulltext_results:
                if product['id'] not in all_results:
                    all_results[product['id']] = product
                    all_results[product['id']]['search_methods'] = ['fulltext']
                    all_results[product['id']]['combined_score'] = product['score'] * 2.0  # Boost for name/desc match
            
            # Strategy 2: Attribute-based search
            attribute_results = self._search_by_attributes(session, query_cleaned, limit * 2)
            for product in attribute_results:
                if product['id'] not in all_results:
                    all_results[product['id']] = product
                    all_results[product['id']]['search_methods'] = ['attribute']
                    all_results[product['id']]['combined_score'] = product['score']
                else:
                    all_results[product['id']]['search_methods'].append('attribute')
                    all_results[product['id']]['combined_score'] += product['score'] * 0.5
            
            # Strategy 3: CONTAINS search as fallback
            if len(all_results) < limit:
                contains_results = self._search_contains(session, query_cleaned, limit)
                for product in contains_results:
                    if product['id'] not in all_results:
                        all_results[product['id']] = product
                        all_results[product['id']]['search_methods'] = ['contains']
                        all_results[product['id']]['combined_score'] = product['score'] * 0.8
                    else:
                        all_results[product['id']]['search_methods'].append('contains')
                        all_results[product['id']]['combined_score'] += product['score'] * 0.3
            
            # Sort by combined score and return top results
            sorted_results = sorted(
                all_results.values(),
                key=lambda x: x['combined_score'],
                reverse=True
            )[:limit]
            
            return sorted_results
    
    def _search_fulltext(self, session, query, limit):

        products = []
        
        # Try different query formats
        query_variations = [
            query,  # Original query
            f'"{query}"',  # Exact phrase
            ' OR '.join(query.split()),  # OR between words
            ' AND '.join(query.split()),  # AND between words
            ' '.join([f'{word}~' for word in query.split()]),  # Fuzzy for each word
            ' '.join([f'{word}~2' for word in query.split()]),  # More fuzzy
        ]
        
        for query_variant in query_variations:
            if products:  # Stop if we found results
                break
                
            try:
                cypher_query = """
                    CALL db.index.fulltext.queryNodes('product_search', $search_term)
                    YIELD node, score
                    RETURN node.id as product_id, 
                           node.name as product_name,
                           node.short_description as description,
                           score
                    ORDER BY score DESC
                    LIMIT $limit
                """
                
                result = session.run(cypher_query, search_term=query_variant, limit=limit)
                
                for record in result:
                    product = {
                        'id': record['product_id'],
                        'name': record['product_name'],
                        'description': record['description'] or "",
                        'score': record['score']
                    }
                    
                    # Avoid duplicates
                    if not any(p['id'] == product['id'] for p in products):
                        products.append(product)
                        
            except Exception as e:
                # Continue to next variation if this one fails
                continue
        
        return products[:limit]
    
    def _search_by_attributes(self, session, query, limit):

        # Search for products that have attributes matching the query
        cypher_query = """
            MATCH (p:Product)-[:HAS_ATTRIBUTE]->(a:Attribute)
            WHERE toLower(a.key) CONTAINS toLower($search_term)
               OR toLower(a.value) CONTAINS toLower($search_term)
            WITH p, 
                 COUNT(DISTINCT a) as matching_attrs,
                 COLLECT(DISTINCT {key: a.key, value: a.value, type: a.type}) as matched_attributes
            RETURN p.id as product_id,
                   p.name as product_name,
                   p.short_description as description,
                   matching_attrs as score,
                   matched_attributes
            ORDER BY score DESC
            LIMIT $limit
        """
        
        try:
            result = session.run(cypher_query, search_term=query, limit=limit)
            products = []
            
            for record in result:
                product = {
                    'id': record['product_id'],
                    'name': record['product_name'],
                    'description': record['description'] or "",
                    'score': float(record['score']),  # Number of matching attributes
                    'matched_attributes': record['matched_attributes']
                }
                products.append(product)
            
            return products
        except Exception as e:
            print(f"Attribute search error: {e}")
            return []
    
    def _search_contains(self, session, query, limit):

        cypher_query = """
            MATCH (p:Product)
            WHERE toLower(p.name) CONTAINS toLower($search_term)
               OR (p.short_description IS NOT NULL AND toLower(p.short_description) CONTAINS toLower($search_term))
            WITH p, 
                 CASE 
                    WHEN toLower(p.name) CONTAINS toLower($search_term) THEN 2.0
                    ELSE 1.0
                 END as score
            RETURN p.id as product_id,
                   p.name as product_name,
                   p.short_description as description,
                   score
            ORDER BY score DESC, p.name
            LIMIT $limit
        """
        
        try:
            result = session.run(cypher_query, search_term=query, limit=limit)
            products = []
            
            for record in result:
                products.append({
                    'id': record['product_id'],
                    'name': record['product_name'],
                    'description': record['description'] or "",
                    'score': record['score']
                })
            
            return products
        except Exception as e:
            print(f"Contains search error: {e}")
            return []
    
    def get_product_details(self, product_id):

        with self.driver.session() as session:
            cypher_query = """
                MATCH (p:Product {id: $product_id})
                OPTIONAL MATCH (p)-[:HAS_ATTRIBUTE]->(a:Attribute)
                WITH p, COLLECT(DISTINCT {key: a.key, value: a.value, type: a.type}) as attributes
                RETURN p.id as id,
                       p.name as name,
                       p.short_description as description,
                       attributes
            """
            
            result = session.run(cypher_query, product_id=product_id)
            record = result.single()
            
            if record:
                return {
                    'id': record['id'],
                    'name': record['name'],
                    'description': record['description'] or "",
                    'attributes': record['attributes']
                }
            return None
    
    def format_search_results(self, products, show_attributes=False):

        if not products:
            return "No products found."
        
        output = ["\n" + "=" * 80]
        output.append(f"Found {len(products)} product(s):")
        output.append("=" * 80)
        
        for i, product in enumerate(products, 1):
            output.append(f"\n{i}. Product ID: {product['id']}")
            output.append(f"   Name: {product['name']}")
            
            desc = product['description']
            if desc and len(desc) > 100:
                desc = desc[:97] + "..."
            if desc:
                output.append(f"   Description: {desc}")
            
            # Show which search methods found this product
            if 'search_methods' in product:
                methods = ', '.join(product['search_methods'])
                output.append(f"   Found by: {methods}")
            
            # Show matched attributes if available
            if show_attributes and 'matched_attributes' in product and product['matched_attributes']:
                output.append("   Matched Attributes:")
                for attr in product['matched_attributes'][:3]:  # Show first 3 attributes
                    output.append(f"     - {attr['key']}: {attr['value']} ({attr['type']})")
                if len(product['matched_attributes']) > 3:
                    output.append(f"     ... and {len(product['matched_attributes']) - 3} more attributes")
            
            output.append(f"   Relevance Score: {product.get('combined_score', product['score']):.3f}")
        
        output.append("=" * 80)
        
        return "\n".join(output)
    
    def run_interactive_search(self):
        
        while True:
            try:
                # Get user input
                print("\n")
                query = input("Enter search query: ").strip()
                
                # Check for exit command
                if query.lower() == 'exit':
                    print("\nExiting search system.")
                    break
                
                # Skip empty queries
                if not query:
                    print("Enter a search query.")
                    continue
                
                # Perform search
                print(f"\nSearching for: '{query}'")
                products = self.search_products(query)
                
                # Display results
                if products:
                    # Print just the top 10 product IDs as requested
                    print("\nTop 10 Product IDs:")
                    print("-" * 40)
                    for i, product in enumerate(products[:10], 1):
                        print(f"{i:2}. {product['id']}")
                    
                    # Also show detailed results with attributes
                    print(self.format_search_results(products, show_attributes=True))
                else:
                    print("No products found.")
                
            except KeyboardInterrupt:
                print("\n\nInterrupted by user.")
                break
            except EOFError:
                print("\n\nEnd of input. Exiting.")
                break
            except Exception as e:
                print(f"Error during search: {e}")

def main():

    neo4j_uri = os.environ.get('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_user = os.environ.get('NEO4J_USER', 'neo4j')
    neo4j_password = os.environ.get('NEO4J_PASSWORD', 'password')
    
    # Initialize search system
    search_system = ProductSearchSystem(neo4j_uri, neo4j_user, neo4j_password)
    
    try:
        # Run interactive search
        search_system.run_interactive_search()
    finally:
        search_system.close()

if __name__ == "__main__":
    main()