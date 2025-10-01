import os
import sys
import re
from typing import List, Dict, Set, Tuple
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
import numpy as np


class HybridSearchSystem:
    
    def __init__(self, 
                 neo4j_uri="bolt://localhost:7687",
                 neo4j_user="neo4j", 
                 neo4j_password="password",
                 qdrant_host="localhost",
                 qdrant_port=6334):
        
        try:
            self.neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
            self._verify_neo4j_connection()
        except Exception as e:
            print(f"Failed to connect to Neo4j: {e}")
            sys.exit(1)
        
        try:
            self.qdrant_client = QdrantClient(host=qdrant_host, port=qdrant_port, timeout=30)
            self.collection_name = "products"
            collection_info = self.qdrant_client.get_collection(self.collection_name)
            print(f"Connected to Qdrant at http://{qdrant_host}:{qdrant_port}")
        except Exception as e:
            print(f"Failed to connect to Qdrant: {e}")
            sys.exit(1)
        
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
    
    def _verify_neo4j_connection(self):
        with self.neo4j_driver.session() as session:
            result = session.run("MATCH (p:Product) RETURN count(p) as count")
            count = result.single()['count']
            if count == 0:
                raise Exception("No products found in Neo4j database")
            print(f"Connected to Neo4j. Found {count} products")
    
    def close(self):
        self.neo4j_driver.close()
    
    def analyze_query(self, query):
        analysis = {
            'has_product_code': False,
            'code_patterns': [],
            'is_short': len(query.split()) <= 3,
            'is_descriptive': len(query.split()) > 5,
            'has_attributes': False,
            'normalized_terms': set()
        }
        
        # Check for product codes (e.g., AIUR-06-102J, CX-112, etc.)
        code_patterns = [
            r'\b[A-Z0-9]+(?:-[A-Z0-9]+)+\b',  # Hyphenated codes
            r'\b(?=.*[A-Z])(?=.*[0-9])[A-Z0-9]{4,}\b'  # Mixed alphanumeric
        ]
        
        for pattern in code_patterns:
            matches = re.findall(pattern, query.upper())
            if matches:
                analysis['has_product_code'] = True
                analysis['code_patterns'].extend(matches)
        
        # Check for attribute-like patterns (key:value, key=value)
        if ':' in query or '=' in query or any(word in query.lower() for word in ['with', 'having', 'type', 'category']):
            analysis['has_attributes'] = True
        
        # Normalize query terms
        analysis['normalized_terms'] = self._normalize_search_terms(query)
        
        return analysis
    
    def _normalize_search_terms(self, query):
        terms = set()
        terms.add(query.lower())
        
        # Handle product codes
        if re.match(r'.*[A-Z0-9]+-[A-Z0-9]+.*', query.upper()):
            terms.add(query.upper())
            terms.add(query.lower())
            terms.add(query.replace('-', '').upper())
            terms.add(query.replace('-', '').lower())
            terms.add(query.replace('-', ' ').upper())
            terms.add(query.replace('-', ' ').lower())
        
        # Split into words
        words = re.split(r'[\s\-_,;:.()]+', query)
        for word in words:
            word = word.strip().lower()
            if len(word) > 1:
                terms.add(word)
        
        return terms
    
    def search_neo4j(self, query, normalized_terms, limit = 20):

        with self.neo4j_driver.session() as session:
            all_results = {}
            
            # Strategy 1: Pre-computed terms search (fastest for codes)
            terms_list = list(normalized_terms)
            cypher_precomputed = """
                MATCH (p:Product)
                WHERE ANY(term IN $search_terms WHERE term IN p.search_terms_list)
                WITH p, SIZE([term IN $search_terms WHERE term IN p.search_terms_list]) as matches
                WHERE matches > 0
                RETURN p.id as product_id, p.name as product_name, 
                       p.short_description as description, matches as score
                ORDER BY score DESC
                LIMIT $limit
            """
            
            try:
                result = session.run(cypher_precomputed, search_terms=terms_list, limit=limit)
                for record in result:
                    pid = record['product_id']
                    if pid not in all_results:
                        all_results[pid] = {
                            'id': pid,
                            'name': record['product_name'],
                            'description': record['description'] or "",
                            'neo4j_score': float(record['score']) * 3.0,
                            'methods': ['precomputed']
                        }
            except Exception as e:
                print(f"Neo4j precomputed search error: {e}")
            
            # Strategy 2: Full-text search
            try:
                cypher_fulltext = """
                    CALL db.index.fulltext.queryNodes('product_search', $search_term)
                    YIELD node, score
                    RETURN node.id as product_id, node.name as product_name,
                           node.short_description as description, score
                    ORDER BY score DESC
                    LIMIT $limit
                """
                
                result = session.run(cypher_fulltext, search_term=query, limit=limit)
                for record in result:
                    pid = record['product_id']
                    if pid not in all_results:
                        all_results[pid] = {
                            'id': pid,
                            'name': record['product_name'],
                            'description': record['description'] or "",
                            'neo4j_score': float(record['score']) * 2.0,
                            'methods': ['fulltext']
                        }
                    else:
                        all_results[pid]['neo4j_score'] += float(record['score']) * 0.5
                        all_results[pid]['methods'].append('fulltext')
            except Exception as e:
                print(f"Neo4j fulltext search error: {e}")
            
            # Strategy 3: Attribute search
            cypher_attributes = """
                MATCH (p:Product)-[:HAS_ATTRIBUTE]->(a:Attribute)
                WHERE ANY(term IN $search_terms WHERE 
                         toLower(a.key) CONTAINS term OR 
                         toLower(a.value) CONTAINS term)
                WITH p, COUNT(DISTINCT a) as matching_attrs
                RETURN p.id as product_id, p.name as product_name,
                       p.short_description as description, matching_attrs as score
                ORDER BY score DESC
                LIMIT $limit
            """
            
            try:
                result = session.run(cypher_attributes, search_terms=terms_list, limit=limit)
                for record in result:
                    pid = record['product_id']
                    if pid not in all_results:
                        all_results[pid] = {
                            'id': pid,
                            'name': record['product_name'],
                            'description': record['description'] or "",
                            'neo4j_score': float(record['score']) * 1.5,
                            'methods': ['attribute']
                        }
                    else:
                        all_results[pid]['neo4j_score'] += float(record['score']) * 0.3
                        all_results[pid]['methods'].append('attribute')
            except Exception as e:
                print(f"Neo4j attribute search error: {e}")
            
            return list(all_results.values())
    
    def search_qdrant(self, query, limit = 20):

        query_vector = self.model.encode(query).tolist()
        
        try:
            results = self.qdrant_client.search(
                collection_name=self.collection_name,
                query_vector=query_vector,
                limit=limit,
                with_payload=True
            )
            
            qdrant_results = []
            for result in results:
                qdrant_results.append({
                    'id': result.payload.get('product_id', 'Unknown'),
                    'name': result.payload.get('name', ''),
                    'description': result.payload.get('short_description', ''),
                    'qdrant_score': result.score
                })
            
            return qdrant_results
        
        except Exception as e:
            print(f"Qdrant search error: {e}")
            return []
    
    def hybrid_search(self, query, limit = 10):

        if not query.strip():
            return []
        
        print(f"\nAnalyzing query: '{query}'")
        
        analysis = self.analyze_query(query)
        
        # Determine weights based on query analysis
        if analysis['has_product_code']:
            # Product codes: heavily favor Neo4j
            neo4j_weight = 0.8
            qdrant_weight = 0.2
            print("  Query type: Product code detected - favoring exact match")

        elif analysis['is_descriptive']:
            # Long descriptive queries: favor Qdrant
            neo4j_weight = 0.3
            qdrant_weight = 0.7
            print("  Query type: Descriptive - favoring semantic search")

        elif analysis['has_attributes']:
            # Attribute-based: favor Neo4j
            neo4j_weight = 0.7
            qdrant_weight = 0.3
            print("  Query type: Attribute-based - favoring graph search")
            
        else:
            # Balanced approach
            neo4j_weight = 0.5
            qdrant_weight = 0.5
            print("  Query type: General - using balanced approach")
        
        # Get results from both systems
        print("  Searching Neo4j")
        neo4j_results = self.search_neo4j(query, analysis['normalized_terms'], limit=20)
        print(f"    Found {len(neo4j_results)} results")
        
        print("  Searching Qdrant...")
        qdrant_results = self.search_qdrant(query, limit=20)
        print(f"    Found {len(qdrant_results)} results")
        
        # Combine and rank using weighted reciprocal rank fusion
        combined_scores = {}
        
        # Process Neo4j results
        for rank, result in enumerate(neo4j_results, 1):
            pid = result['id']
            # Reciprocal rank score with Neo4j internal score boost
            rr_score = 1.0 / (rank + 10)  # Adding 10 to avoid over-weighting top results
            internal_score = result['neo4j_score'] / (max([r['neo4j_score'] for r in neo4j_results]) + 0.001)
            
            combined_scores[pid] = {
                'id': pid,
                'name': result['name'],
                'description': result['description'],
                'neo4j_rank': rank,
                'neo4j_score': internal_score,
                'neo4j_methods': result.get('methods', []),
                'final_score': neo4j_weight * (0.7 * rr_score + 0.3 * internal_score)
            }
        
        # Process Qdrant results
        for rank, result in enumerate(qdrant_results, 1):
            pid = result['id']
            rr_score = 1.0 / (rank + 10)
            
            if pid in combined_scores:
                # Product found in both - boost score
                combined_scores[pid]['qdrant_rank'] = rank
                combined_scores[pid]['qdrant_score'] = result['qdrant_score']
                combined_scores[pid]['final_score'] += qdrant_weight * rr_score
                # Bonus for appearing in both
                combined_scores[pid]['final_score'] *= 1.2
            else:
                combined_scores[pid] = {
                    'id': pid,
                    'name': result['name'],
                    'description': result['description'],
                    'qdrant_rank': rank,
                    'qdrant_score': result['qdrant_score'],
                    'final_score': qdrant_weight * rr_score
                }
        
        # Sort by final score and return top K
        sorted_results = sorted(
            combined_scores.values(),
            key=lambda x: x['final_score'],
            reverse=True
        )[:limit]
        
        return sorted_results
    
    def format_results(self, results, verbose = False):
        """Format search results for display."""
        if not results:
            return "No products found."
        
        output = ["\nTOP 10 SEARCH RESULTS"]
        
        for i, result in enumerate(results, 1):
            output.append(f"\n{i}. Product ID: {result['id']}")
            output.append(f"   Name: {result['name']}")
            
            if verbose:
                if result.get('description'):
                    desc_preview = result['description'][:100] + "..." if len(result['description']) > 100 else result['description']
                    output.append(f"   Description: {desc_preview}")
                
                sources = []
                if 'neo4j_rank' in result:
                    methods = result.get('neo4j_methods', [])
                    if methods:
                        sources.append(f"Neo4j (rank: {result['neo4j_rank']}, methods: {', '.join(methods)})")
                    else:
                        sources.append(f"Neo4j (rank: {result['neo4j_rank']})")
                if 'qdrant_rank' in result:
                    sources.append(f"Qdrant (rank: {result['qdrant_rank']}, score: {result['qdrant_score']:.3f})")
                
                if sources:
                    output.append(f"   Sources: {' | '.join(sources)}")
                
                output.append(f"   Combined Score: {result['final_score']:.4f}")
        
        return "\n".join(output)
    
    def run_interactive_search(self):
        
        verbose = False
        
        while True:
            try:
                query = input("\nEnter search query: ").strip()
                
                if query.lower() == 'exit':
                    break
                
                if query.lower() == 'verbose':
                    verbose = not verbose
                    print(f"Verbose mode: {'ON' if verbose else 'OFF'}")
                    continue
                
                if not query:
                    print("Enter a search query.")
                    continue
                
                results = self.hybrid_search(query, limit=10)
                
                print(self.format_results(results, verbose=verbose))
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error during search: {e}")

def main():

    neo4j_uri = os.environ.get('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_user = os.environ.get('NEO4J_USER', 'neo4j')
    neo4j_password = os.environ.get('NEO4J_PASSWORD', 'password')
    qdrant_host = os.environ.get('QDRANT_HOST', 'localhost')
    qdrant_port = int(os.environ.get('QDRANT_PORT', '6334'))
    
    search_system = HybridSearchSystem(
        neo4j_uri=neo4j_uri,
        neo4j_user=neo4j_user,
        neo4j_password=neo4j_password,
        qdrant_host=qdrant_host,
        qdrant_port=qdrant_port
    )
    
    try:
        search_system.run_interactive_search()
    finally:
        search_system.close()


if __name__ == "__main__":
    main()