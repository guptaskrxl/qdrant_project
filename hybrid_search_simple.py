import os
import sys
import re
from typing import List, Dict, Set
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient

class SimpleHybridSearch:
    
    def __init__(self, 
                 neo4j_uri="bolt://localhost:7687",
                 neo4j_user="neo4j", 
                 neo4j_password="password",
                 qdrant_host="localhost",
                 qdrant_port=6334):
        
        try:
            self.neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
            with self.neo4j_driver.session() as session:
                result = session.run("MATCH (p:Product) RETURN count(p) as count")
                count = result.single()['count']
                print(f"Connected to Neo4j. Found {count} products")
        except Exception as e:
            print(f"Failed to connect to Neo4j: {e}")
            sys.exit(1)
        
        try:
            self.qdrant_client = QdrantClient(host=qdrant_host, port=qdrant_port, timeout=30)
            self.collection_name = "products"
            collection_info = self.qdrant_client.get_collection(self.collection_name)
            print(f"Connected to Qdrant. Found {collection_info.points_count} vectors")
            
            self.model = SentenceTransformer("all-MiniLM-L6-v2")
        except Exception as e:
            print(f"Failed to connect to Qdrant: {e}")
            sys.exit(1)
    
    def close(self):
        self.neo4j_driver.close()
    
    def normalize_search_input(self, query):
        terms = set()
        terms.add(query.lower())
        
        # Handle product codes
        if re.match(r'.*[A-Z0-9]+-[A-Z0-9]+.*', query.upper()):
            terms.add(query.upper())
            terms.add(query.lower())
            no_hyphen = query.replace('-', '')
            terms.add(no_hyphen.upper())
            terms.add(no_hyphen.lower())
            space_version = query.replace('-', ' ')
            terms.add(space_version.upper())
            terms.add(space_version.lower())
        
        # Split into words
        words = re.split(r'[\s\-_,;:.()]+', query)
        for word in words:
            word = word.strip().lower()
            if len(word) > 1:
                terms.add(word)
        
        return terms
    
    def search_neo4j(self, query, limit = 10):

        search_terms = self.normalize_search_input(query)
        terms_list = list(search_terms)
        
        neo4j_results = {}
        
        with self.neo4j_driver.session() as session:
            # 1. Pre-computed terms search
            try:
                result = session.run("""
                    MATCH (p:Product)
                    WHERE ANY(term IN $search_terms WHERE term IN p.search_terms_list)
                    WITH p, SIZE([term IN $search_terms WHERE term IN p.search_terms_list]) as matches
                    WHERE matches > 0
                    RETURN p.id as product_id, p.name as product_name, 
                           p.short_description as description, matches as score
                    ORDER BY score DESC
                    LIMIT $limit
                """, search_terms=terms_list, limit=limit)
                
                for record in result:
                    pid = record['product_id']
                    neo4j_results[pid] = {
                        'id': pid,
                        'name': record['product_name'],
                        'score': float(record['score']) * 3.0  
                    }
            except Exception as e:
                print(f"Neo4j precomputed search error: {e}")
            
            # 2. Full-text search
            try:
                result = session.run("""
                    CALL db.index.fulltext.queryNodes('product_search', $search_term)
                    YIELD node, score
                    RETURN node.id as product_id, node.name as product_name, score
                    ORDER BY score DESC
                    LIMIT $limit
                """, search_term=query, limit=limit)
                
                for record in result:
                    pid = record['product_id']
                    if pid in neo4j_results:
                        neo4j_results[pid]['score'] += float(record['score']) * 2.0
                    else:
                        neo4j_results[pid] = {
                            'id': pid,
                            'name': record['product_name'],
                            'score': float(record['score']) * 2.0
                        }
            except Exception as e:
                print(f"Neo4j fulltext search error: {e}")
            
            # 3. Attribute search
            try:
                result = session.run("""
                    MATCH (p:Product)-[:HAS_ATTRIBUTE]->(a:Attribute)
                    WHERE ANY(term IN $search_terms WHERE 
                             toLower(a.key) CONTAINS term OR 
                             toLower(a.value) CONTAINS term)
                    WITH p, COUNT(DISTINCT a) as matching_attrs
                    RETURN p.id as product_id, p.name as product_name, matching_attrs as score
                    ORDER BY score DESC
                    LIMIT $limit
                """, search_terms=terms_list, limit=limit)
                
                for record in result:
                    pid = record['product_id']
                    if pid in neo4j_results:
                        neo4j_results[pid]['score'] += float(record['score']) * 1.2
                    else:
                        neo4j_results[pid] = {
                            'id': pid,
                            'name': record['product_name'],
                            'score': float(record['score']) * 1.2
                        }
            except Exception as e:
                print(f"Neo4j attribute search error: {e}")
        
        return neo4j_results
    
    def search_qdrant(self, query, limit = 10):

        query_vector = self.model.encode(query).tolist()
        qdrant_results = {}
        
        try:
            results = self.qdrant_client.search(
                collection_name=self.collection_name,
                query_vector=query_vector,
                limit=limit,
                with_payload=True
            )
            
            for result in results:
                pid = result.payload.get('product_id', 'Unknown')
                qdrant_results[pid] = {
                    'id': pid,
                    'name': result.payload.get('name', ''),
                    'score': result.score 
                }
            
        except Exception as e:
            print(f"Qdrant search error: {e}")
        
        return qdrant_results
    
    def normalize_scores(self, scores_dict, score_key = 'score'):

        if not scores_dict:
            return scores_dict
        
        scores = [item[score_key] for item in scores_dict.values()]
        min_score = min(scores)
        max_score = max(scores)
        
        if max_score == min_score:
            for item in scores_dict.values():
                item['normalized_score'] = 1.0
        else:
            for item in scores_dict.values():
                item['normalized_score'] = (item[score_key] - min_score) / (max_score - min_score)
        
        return scores_dict
    
    def hybrid_search(self, query, neo4j_weight = 0.5, qdrant_weight = 0.5):

        if not query.strip():
            return []
        
        print("\nQuerying Neo4j:")
        neo4j_results = self.search_neo4j(query, limit=20)
        print(f" Found {len(neo4j_results)} products")
        
        print("\nQuerying Qdrant:")
        qdrant_results = self.search_qdrant(query, limit=20)
        print(f" Found {len(qdrant_results)} products")
        
        if neo4j_results:
            neo4j_results = self.normalize_scores(neo4j_results)
        if qdrant_results:
            qdrant_results = self.normalize_scores(qdrant_results)
        
        combined_results = {}
        
        for pid, data in neo4j_results.items():
            combined_results[pid] = {
                'id': pid,
                'name': data['name'],
                'neo4j_score': data['normalized_score'],
                'qdrant_score': 0,
                'combined_score': neo4j_weight * data['normalized_score']
            }
        
        for pid, data in qdrant_results.items():
            if pid in combined_results:
                # Product found in both systems
                combined_results[pid]['qdrant_score'] = data['normalized_score']
                combined_results[pid]['combined_score'] = (
                    neo4j_weight * combined_results[pid]['neo4j_score'] +
                    qdrant_weight * data['normalized_score']
                )
            else:
                # Product only in Qdrant
                combined_results[pid] = {
                    'id': pid,
                    'name': data['name'],
                    'neo4j_score': 0,
                    'qdrant_score': data['normalized_score'],
                    'combined_score': qdrant_weight * data['normalized_score']
                }
        
        sorted_results = sorted(
            combined_results.values(),
            key=lambda x: x['combined_score'],
            reverse=True
        )[:10]
        
        return sorted_results
    
    def format_results(self, results):
        if not results:
            return "\nNo products found."
        
        output = ["\nTop 10 matching products"]
        
        for i, result in enumerate(results, 1):
            output.append(f"\n{i:2}. Product ID: {result['id']}")
            output.append(f"    Name: {result['name']}")
            output.append(f"    Combined Score: {result['combined_score']:.4f}")
            output.append(f"    (Neo4j: {result['neo4j_score']:.3f}, Qdrant: {result['qdrant_score']:.3f})")
        
        return "\n".join(output)
    
    def run_interactive_search(self):
        
        neo4j_weight = 0.5
        qdrant_weight = 0.5
        
        while True:
            try:
                query = input("\nEnter search query: ").strip()
                
                if query.lower() == 'exit':
                    break
                
                results = self.hybrid_search(query, neo4j_weight, qdrant_weight)
                
                print(self.format_results(results))
                
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
    
    search_system = SimpleHybridSearch(
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