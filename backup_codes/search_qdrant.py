import sys
from typing import List, Optional
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient

class ProductSearchEngine:
    
    def __init__(self, host: str = "localhost", port: int = 6334):

        self.collection_name = "products"
        self.model_name = "all-MiniLM-L6-v2"

        self.model = SentenceTransformer(self.model_name)
        
        # Initialize Qdrant client
        try:
            self.client = QdrantClient(
                host=host,
                port=port,
                timeout=30
            )
            # Test connection and collection
            collection_info = self.client.get_collection(self.collection_name)
            print(f"Connected to Qdrant at http://{host}:{port}")
            print(f"Collection '{self.collection_name}' found with {collection_info.vectors_count} vectors")
        except Exception as e:
            print(f"Failed to connect to Qdrant or access collection: {e}")
            sys.exit(1)
    
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
        
        output = ["\nTop 10 most similar product IDs:"]
        output.append("-" * 40)
        
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
        
        output.append("-" * 40)
        return "\n".join(output)

def clear_screen():
    """Clear the terminal screen."""
    import os
    os.system('cls' if os.name == 'nt' else 'clear')

def main():

    # Initialize search engine
    try:
        engine = ProductSearchEngine()
    except Exception as e:
        print(f"Failed to initialize search engine: {e}")
        sys.exit(1)
    
    print("\nReady for searches!\n")
    
    # Interactive search loop
    while True:
        try:
            # Get user input
            query = input("Enter a product query (or type 'exit' to quit): ").strip()
            
            # Check for special commands
            if query.lower() == 'exit':
                break
            
            # Perform search
            print(f"\nSearching for: '{query}'...")
            results = engine.search(query, limit=10)
            
            # Display results
            formatted_results = engine.format_results(results)
            print(formatted_results)
            print()
            
        except KeyboardInterrupt:
            print("\n\nInterrupted by user. Exiting...")
            break
        except Exception as e:
            print(f"✗ An error occurred: {e}")
            print("Please try again or type 'exit' to quit.")

if __name__ == "__main__":
    main()