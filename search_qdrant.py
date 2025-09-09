import sys
from typing import List, Optional
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue


class ProductSearchEngine:
    """Product search engine using Qdrant vector database."""
    
    def __init__(self, host: str = "localhost", port: int = 6334):

        self.collection_name = "products"
        self.model_name = "all-MiniLM-L6-v2"
        
        # Initialize Qdrant client
        try:
            self.client = QdrantClient(
                host=host,
                port=port,
                timeout=30
            )
            # Test connection and collection
            collection_info = self.client.get_collection(self.collection_name)
            print(f"\nConnected to Qdrant at http://{host}:{port}")
        except Exception as e:
            print(f"\nQdrant error: {e}")
    
    def search(self, query: str, limit: int = 10) -> List[dict]:
        # Generate query embedding
        query_vector = self.model.encode(query).tolist()
        
        # Perform search
        try:
            results = self.client.search(
                collection_name=self.collection_name,
                query_vector=query_vector,
                limit=limit,
                with_payload=True
            )
            
            return results
        except Exception as e:
            print(f"Search error: {e}")
            return []
    
    def format_results(self, results: List) -> str:
        if not results:
            return "No matching products found."
        
        output = ["\nTop 10 most similar product"]
        
        for i, result in enumerate(results, 1):
            product_id = result.payload.get('product_id', 'Unknown')
            score = result.score
            
            # Include product name if available
            name = result.payload.get('name', '')
            if name:
                output.append(f"{i:2}. ID: {product_id} (Score: {score:.4f})")
                output.append(f"    Name: {name}")
            else:
                output.append(f"{i:2}. ID: {product_id} (Score: {score:.4f})")
        
        return "\n\n".join(output)

def clear_screen():
    """Clear the terminal screen."""
    import os
    os.system('cls' if os.name == 'nt' else 'clear')

def main():

    try:
        engine = ProductSearchEngine()
    except Exception as e:
        print(f"Failed to initialize search engine: {e}")
        sys.exit(1)

    while True:
        try:
            # user input
            query = input("\nEnter a product query (or type 'exit' to quit): ").strip()
            
            if query.lower() == 'exit':
                break
            
            results = engine.search(query, limit=10)
            
            # Display results
            formatted_results = engine.format_results(results)
            print(formatted_results)
            print()
            
        except KeyboardInterrupt:
            print("\n\nKeyboard interrupt")
            break

if __name__ == "__main__":
    main()