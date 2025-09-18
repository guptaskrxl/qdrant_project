import os
import sys
import re
from neo4j import GraphDatabase
from typing import List, Dict, Set, Tuple


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
                    print("Warning: Full-text search index not found. Creating it now.")
                    self.create_search_index(session)
                    
        except Exception as e:
            print(f"Error connecting to Neo4j: {e}")
            sys.exit(1)
    
    def create_search_index(self, session):
        """Create the full-text search index if it doesn't exist"""
        try:
            session.run("DROP INDEX product_search IF EXISTS")
        except:
            pass
        
        session.run("""
            CREATE FULLTEXT INDEX product_search 
            FOR (p:Product)
            ON EACH [p.name, p.short_description]
        """)
        print("Full-text search index created")
    
    def extract_product_codes(self, text):
        codes = []
        
        # Pattern 1: Alphanumeric with hyphens (keep as-is)
        # Matches: AIUR-06-102J, CX-112, BTD-5000
        pattern1 = r'\b[A-Z0-9]+(?:-[A-Z0-9]+)+\b'
        found_codes = re.findall(pattern1, text.upper())
        codes.extend(found_codes)
        
        # Pattern 2: Mixed alphanumeric without hyphens but looks like a code
        # At least one letter and one number, 4+ characters
        pattern2 = r'\b(?=.*[A-Z])(?=.*[0-9])[A-Z0-9]{4,}\b'
        potential_codes = re.findall(pattern2, text.upper())
        for code in potential_codes:
            if code not in codes:
                codes.append(code)
        
        return codes
    
    def normalize_product_code(self, code):
    
        # Generate variations of a product code for matching.

        variations = [code]
        
        # Remove hyphens version
        no_hyphen = code.replace('-', '')
        if no_hyphen != code:
            variations.append(no_hyphen)
        
        # Space-separated version
        space_version = code.replace('-', ' ')
        if space_version != code:
            variations.append(space_version)
        
        return variations
    
    def tokenize_query(self, query):

        codes = self.extract_product_codes(query)
        
        # Create a modified query where codes are replaced with placeholders
        modified_query = query
        code_placeholders = {}
        for i, code in enumerate(codes):
            placeholder = f"__CODE{i}__"
            code_placeholders[placeholder] = code
            # Replace all variations of the code in the query
            for variation in self.normalize_product_code(code):
                modified_query = re.sub(r'\b' + re.escape(variation) + r'\b', 
                                       placeholder, modified_query, flags=re.IGNORECASE)
        
        # Now tokenize the modified query
        words = []
        tokens = re.split(r'[\s\-_,;:.()]+', modified_query)
        
        for token in tokens:
            token = token.strip()
            if token and not token.startswith('__CODE'):
                if len(token) > 1:  # Skip single characters
                    words.append(token.lower())
        
        return {
            'codes': codes,
            'words': words,
            'full_query': query
        }
    
    def search_products(self, query_string, limit=10):
        if not query_string.strip():
            return []
        
        with self.driver.session() as session:
            # Tokenize the query intelligently
            query_parts = self.tokenize_query(query_string)
            
            all_results = {}
            
            # Strategy 1: Product code exact/fuzzy match (highest priority)
            if query_parts['codes']:
                code_results = self._search_by_codes(session, query_parts['codes'], limit)
                for product in code_results:
                    if product['id'] not in all_results:
                        all_results[product['id']] = product
                        all_results[product['id']]['search_methods'] = ['code_match']
                        all_results[product['id']]['combined_score'] = product['score'] * 3.0  # Highest boost
                    else:
                        all_results[product['id']]['search_methods'].append('code_match')
                        all_results[product['id']]['combined_score'] += product['score'] * 1.5
            
            # Strategy 2: Full-text search on name and description
            fulltext_results = self._search_fulltext_enhanced(session, query_string, query_parts, limit * 2)
            for product in fulltext_results:
                if product['id'] not in all_results:
                    all_results[product['id']] = product
                    all_results[product['id']]['search_methods'] = ['fulltext']
                    all_results[product['id']]['combined_score'] = product['score'] * 2.0
                else:
                    all_results[product['id']]['search_methods'].append('fulltext')
                    all_results[product['id']]['combined_score'] += product['score'] * 0.7
            
            # Strategy 3: Word-by-word matching
            if query_parts['words']:
                word_results = self._search_by_words(session, query_parts['words'], limit * 2)
                for product in word_results:
                    if product['id'] not in all_results:
                        all_results[product['id']] = product
                        all_results[product['id']]['search_methods'] = ['word_match']
                        all_results[product['id']]['combined_score'] = product['score'] * 1.5
                    else:
                        all_results[product['id']]['search_methods'].append('word_match')
                        all_results[product['id']]['combined_score'] += product['score'] * 0.5
            
            # Strategy 4: Attribute-based search (search individual words in attributes)
            attribute_results = self._search_attributes_enhanced(session, query_parts, limit * 2)
            for product in attribute_results:
                if product['id'] not in all_results:
                    all_results[product['id']] = product
                    all_results[product['id']]['search_methods'] = ['attribute']
                    all_results[product['id']]['combined_score'] = product['score'] * 1.2
                else:
                    all_results[product['id']]['search_methods'].append('attribute')
                    all_results[product['id']]['combined_score'] += product['score'] * 0.4
            
            # Sort by combined score and return top results
            sorted_results = sorted(
                all_results.values(),
                key=lambda x: x['combined_score'],
                reverse=True
            )[:limit]
            
            return sorted_results
    
    def _search_by_codes(self, session, codes, limit):

        all_results = []
        
        for code in codes:
            # Generate variations of the code
            variations = self.normalize_product_code(code)
            
            # Search for each variation
            for variation in variations:
                cypher_query = """
                    MATCH (p:Product)
                    WHERE toLower(p.name) CONTAINS toLower($code)
                       OR (p.short_description IS NOT NULL AND 
                           toLower(p.short_description) CONTAINS toLower($code))
                    WITH p,
                         CASE 
                            WHEN toLower(p.name) CONTAINS toLower($code) THEN 3.0
                            ELSE 1.0
                         END as score
                    RETURN p.id as product_id,
                           p.name as product_name,
                           p.short_description as description,
                           score
                    ORDER BY score DESC
                    LIMIT $limit
                """
                
                try:
                    result = session.run(cypher_query, code=variation, limit=limit)
                    
                    for record in result:
                        # Check if product already in results
                        existing = next((r for r in all_results if r['id'] == record['product_id']), None)
                        if not existing:
                            all_results.append({
                                'id': record['product_id'],
                                'name': record['product_name'],
                                'description': record['description'] or "",
                                'score': record['score'],
                                'matched_code': code
                            })
                except Exception as e:
                    continue
        
        return all_results[:limit]
    
    def _search_fulltext_enhanced(self, session, original_query, query_parts, limit):
        """Enhanced full-text search with better handling of product codes."""
        products = []
        
        # Build query variations
        query_variations = [
            original_query,  # Original
            f'"{original_query}"',  # Exact phrase
        ]
        
        # Add variations with just words (no codes)
        if query_parts['words']:
            words_only = ' '.join(query_parts['words'])
            query_variations.append(words_only)
            query_variations.append(' OR '.join(query_parts['words']))
            query_variations.append(' AND '.join(query_parts['words']))
            
            # Fuzzy matching for words
            fuzzy_words = ' '.join([f'{word}~' for word in query_parts['words']])
            query_variations.append(fuzzy_words)
            fuzzy_words_2 = ' '.join([f'{word}~2' for word in query_parts['words']])
            query_variations.append(fuzzy_words_2)
        
        for query_variant in query_variations:
            if len(products) >= limit:
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
                    
                    if not any(p['id'] == product['id'] for p in products):
                        products.append(product)
                        
            except Exception as e:
                continue
        
        return products[:limit]
    
    def _search_by_words(self, session, words, limit):
        """Search for products matching individual words."""
        if not words:
            return []
        
        # Build WHERE clause for each word
        where_conditions = []
        for word in words:
            where_conditions.append(f"toLower(p.name) CONTAINS toLower('{word}')")
            where_conditions.append(f"(p.short_description IS NOT NULL AND toLower(p.short_description) CONTAINS toLower('{word}'))")
        
        cypher_query = f"""
            MATCH (p:Product)
            WHERE {' OR '.join(where_conditions)}
            WITH p,
                 {' + '.join([f"CASE WHEN toLower(p.name) CONTAINS toLower('{word}') THEN 2 ELSE 0 END" for word in words])} +
                 {' + '.join([f"CASE WHEN p.short_description IS NOT NULL AND toLower(p.short_description) CONTAINS toLower('{word}') THEN 1 ELSE 0 END" for word in words])} as score
            WHERE score > 0
            RETURN p.id as product_id,
                   p.name as product_name,
                   p.short_description as description,
                   score
            ORDER BY score DESC
            LIMIT {limit}
        """
        
        try:
            result = session.run(cypher_query)
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
            print(f"Word search error: {e}")
            return []
    
    def _search_attributes_enhanced(self, session, query_parts, limit):
        """Enhanced attribute search using individual words and codes."""
        search_terms = query_parts['words'] + query_parts['codes']
        
        if not search_terms:
            return []
        
        # Build WHERE clause for attributes
        where_conditions = []
        for term in search_terms:
            where_conditions.append(f"toLower(a.key) CONTAINS toLower('{term}')")
            where_conditions.append(f"toLower(a.value) CONTAINS toLower('{term}')")
        
        cypher_query = f"""
            MATCH (p:Product)-[:HAS_ATTRIBUTE]->(a:Attribute)
            WHERE {' OR '.join(where_conditions)}
            WITH p, 
                 COUNT(DISTINCT a) as matching_attrs,
                 COLLECT(DISTINCT {{key: a.key, value: a.value, type: a.type}}) as matched_attributes
            RETURN p.id as product_id,
                   p.name as product_name,
                   p.short_description as description,
                   matching_attrs as score,
                   matched_attributes
            ORDER BY score DESC
            LIMIT {limit}
        """
        
        try:
            result = session.run(cypher_query)
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
            
            # desc = product['description']
            # if desc and len(desc) > 100:
            #     desc = desc[:97] + "..."
            # if desc:
            #     output.append(f"   Description: {desc}")
            
            # # Show which search methods found this product
            # if 'search_methods' in product:
            #     methods = ', '.join(product['search_methods'])
            #     output.append(f"   Found by: {methods}")
            
            # Show matched code if present
            if 'matched_code' in product:
                output.append(f"   Matched Code: {product['matched_code']}")
            
            # Show matched attributes if available
            if show_attributes and 'matched_attributes' in product and product['matched_attributes']:
                output.append("   Matched Attributes:")
                for attr in product['matched_attributes'][:3]:
                    output.append(f"     - {attr['key']}: {attr['value']} ({attr['type']})")
                if len(product['matched_attributes']) > 3:
                    output.append(f"     ... and {len(product['matched_attributes']) - 3} more attributes")
            
            output.append(f"   Relevance Score: {product.get('combined_score', product['score']):.3f}")
        
        return "\n".join(output)
    
    def run_interactive_search(self):
        
        while True:
            try:
                print("\n")
                query = input("Enter search query: ").strip()
                
                if query.lower() == 'exit':
                    print("\nExiting search system.")
                    break
                
                if not query:
                    print("Enter a search query.")
                    continue
                
                print(f"\nSearching for: '{query}'")
                products = self.search_products(query)
                
                if products:
                    print("\nTop 10 Product IDs:")
                    
                    print(self.format_search_results(products, show_attributes=True))
                else:
                    print("No products found.")
                
            except KeyboardInterrupt:
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