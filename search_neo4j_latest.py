import os
import sys
import re
from neo4j import GraphDatabase
from typing import List, Dict, Set


class ProductSearchSystem:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.verify_connection()
        
    def close(self):
        self.driver.close()
        
    def verify_connection(self):
        try:
            with self.driver.session() as session:
                result = session.run("MATCH (p:Product) RETURN count(p) as count")
                count = result.single()['count']
                
                if count == 0:
                    print("Warning: No products found in database.")
                    sys.exit(1)
                else:
                    print(f"\nConnected to Neo4j.\nFound {count} products in database.")
                
                result = session.run("MATCH (a:Attribute) RETURN count(a) as count")
                attr_count = result.single()['count']
                    
        except Exception as e:
            print(f"Error connecting to Neo4j: {e}")
            sys.exit(1)
    
    def normalize_search_input(self, query):
        """
        Normalize search input to match pre-computed terms.
        Much simpler than before - just extract terms and basic variations.
        """
        terms = set()
        
        # Add the full query lowercased
        terms.add(query.lower())
        
        # Check if it looks like a product code
        # Pattern for codes with hyphens
        if re.match(r'.*[A-Z0-9]+-[A-Z0-9]+.*', query.upper()):
            # Add original
            terms.add(query.upper())
            terms.add(query.lower())
            
            # Add without hyphens
            no_hyphen = query.replace('-', '')
            terms.add(no_hyphen.upper())
            terms.add(no_hyphen.lower())
            
            # Add with spaces
            space_version = query.replace('-', ' ')
            terms.add(space_version.upper())
            terms.add(space_version.lower())
        
        # Split into words and add them
        words = re.split(r'[\s\-_,;:.()]+', query)
        for word in words:
            word = word.strip().lower()
            if len(word) > 1:
                terms.add(word)
        
        return terms
    
    def search_products(self, query_string, limit=10):
        if not query_string.strip():
            return []
        
        with self.driver.session() as session:
            # Get normalized search terms
            search_terms = self.normalize_search_input(query_string)
            
            all_results = {}
            
            # Strategy 1: Direct match in pre-computed search_terms_list
            direct_results = self._search_precomputed_terms(session, search_terms, limit)
            for product in direct_results:
                if product['id'] not in all_results:
                    all_results[product['id']] = product
                    all_results[product['id']]['search_methods'] = ['precomputed_match']
                    all_results[product['id']]['combined_score'] = product['score'] * 3.0
            
            # Strategy 2: Full-text search (now includes search_terms field)
            fulltext_results = self._search_fulltext_simple(session, query_string, limit)
            for product in fulltext_results:
                if product['id'] not in all_results:
                    all_results[product['id']] = product
                    all_results[product['id']]['search_methods'] = ['fulltext']
                    all_results[product['id']]['combined_score'] = product['score'] * 2.0
                else:
                    all_results[product['id']]['search_methods'].append('fulltext')
                    all_results[product['id']]['combined_score'] += product['score'] * 0.5
            
            # Strategy 3: Attribute search (simplified)
            attribute_results = self._search_attributes_simple(session, search_terms, limit)
            for product in attribute_results:
                if product['id'] not in all_results:
                    all_results[product['id']] = product
                    all_results[product['id']]['search_methods'] = ['attribute']
                    all_results[product['id']]['combined_score'] = product['score'] * 1.2
                else:
                    all_results[product['id']]['search_methods'].append('attribute')
                    all_results[product['id']]['combined_score'] += product['score'] * 0.3
            
            # Sort by combined score
            sorted_results = sorted(
                all_results.values(),
                key=lambda x: x['combined_score'],
                reverse=True
            )[:limit]
            
            return sorted_results
    
    def _search_precomputed_terms(self, session, search_terms, limit):
        """
        Search using pre-computed search terms.
        This is MUCH faster than generating variations at runtime.
        """
        # Convert set to list for Cypher
        terms_list = list(search_terms)
        
        cypher_query = """
            MATCH (p:Product)
            WHERE ANY(term IN $search_terms WHERE term IN p.search_terms_list)
            WITH p, 
                 SIZE([term IN $search_terms WHERE term IN p.search_terms_list]) as matches
            WHERE matches > 0
            RETURN p.id as product_id,
                   p.name as product_name,
                   p.short_description as description,
                   matches as score
            ORDER BY score DESC
            LIMIT $limit
        """
        
        try:
            result = session.run(cypher_query, search_terms=terms_list, limit=limit)
            products = []
            
            for record in result:
                products.append({
                    'id': record['product_id'],
                    'name': record['product_name'],
                    'description': record['description'] or "",
                    'score': float(record['score'])
                })
            
            return products
        except Exception as e:
            print(f"Precomputed search error: {e}")
            return []
    
    def _search_fulltext_simple(self, session, query, limit):
        """
        Simple full-text search - now searches across name, description, AND search_terms.
        """
        try:
            # Try exact phrase first
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
            
            result = session.run(cypher_query, search_term=query, limit=limit)
            products = []
            
            for record in result:
                products.append({
                    'id': record['product_id'],
                    'name': record['product_name'],
                    'description': record['description'] or "",
                    'score': record['score']
                })
            
            # If no results, try with fuzzy matching
            if not products:
                words = query.split()
                fuzzy_query = ' '.join([f'{word}~' for word in words])
                result = session.run(cypher_query, search_term=fuzzy_query, limit=limit)
                
                for record in result:
                    products.append({
                        'id': record['product_id'],
                        'name': record['product_name'],
                        'description': record['description'] or "",
                        'score': record['score']
                    })
            
            return products
        except Exception as e:
            return []
    
    def _search_attributes_simple(self, session, search_terms, limit):
        """
        Simplified attribute search using pre-normalized terms.
        """
        terms_list = list(search_terms)
        
        cypher_query = """
            MATCH (p:Product)-[:HAS_ATTRIBUTE]->(a:Attribute)
            WHERE ANY(term IN $search_terms WHERE 
                     toLower(a.key) CONTAINS term OR 
                     toLower(a.value) CONTAINS term)
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
            result = session.run(cypher_query, search_terms=terms_list, limit=limit)
            products = []
            
            for record in result:
                product = {
                    'id': record['product_id'],
                    'name': record['product_name'],
                    'description': record['description'] or "",
                    'score': float(record['score']),
                    'matched_attributes': record['matched_attributes']
                }
                products.append(product)
            
            return products
        except Exception as e:
            print(f"Attribute search error: {e}")
            return []
    
    def format_search_results(self, products, show_attributes=False):
        if not products:
            return "No products found."
        
        output = []
        
        for i, product in enumerate(products, 1):
            output.append(f"\n{i}. Product ID: {product['id']}")
            output.append(f"   Name: {product['name']}")
            
            if 'search_methods' in product:
                methods = ', '.join(product['search_methods'])
                output.append(f"   Found by: {methods}")
            
            if show_attributes and 'matched_attributes' in product and product['matched_attributes']:
                output.append("   Matched Attributes:")
                for attr in product['matched_attributes'][:3]:
                    output.append(f"     - {attr['key']}: {attr['value']} ({attr['type']})")
                if len(product['matched_attributes']) > 3:
                    output.append(f"     ... and {len(product['matched_attributes']) - 3} more attributes")
            
            output.append(f"   Score: {product.get('combined_score', product['score']):.3f}")
        
        return "\n".join(output)
    
    def run_interactive_search(self):
        
        while True:
            try:
                query = input("\nEnter search query: ").strip()
                
                if query.lower() == 'exit':
                    print("\nExiting search system.")
                    break
                
                if not query:
                    print("Enter a search query.")
                    continue
                
                print(f"\nSearching for: '{query}'")
                products = self.search_products(query)
                
                if products:
                    print("\nTop 10 Results:")
                    print(self.format_search_results(products, show_attributes=True))
                else:
                    print("No products found.")
                
            except KeyboardInterrupt:
                print("\n\nExiting...")
                break
            except Exception as e:
                print(f"Error during search: {e}")


def main():
    neo4j_uri = os.environ.get('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_user = os.environ.get('NEO4J_USER', 'neo4j')
    neo4j_password = os.environ.get('NEO4J_PASSWORD', 'password')
    
    search_system = ProductSearchSystem(neo4j_uri, neo4j_user, neo4j_password)
    
    try:
        search_system.run_interactive_search()
    finally:
        search_system.close()


if __name__ == "__main__":
    main()