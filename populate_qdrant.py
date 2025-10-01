import json
import sys
import os
from typing import List, Dict, Any
import pandas as pd
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from tqdm import tqdm


class QdrantProductLoader:
    """
    A modular Qdrant product loader for vector search functionality.
    Collection is created at instantiation; data is always appended to existing collection.
    """
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 6334,
                 collection_name: str = "products",
                 model_name: str = "all-MiniLM-L6-v2",
                 vector_size: int = 384):
        """
        Initialize Qdrant client, embedding model, and ensure collection exists.
        
        Args:
            host: Qdrant server host
            port: Qdrant server port
            collection_name: Name of the collection to use
            model_name: SentenceTransformer model name
            vector_size: Expected vector dimension size
        """
        self.host = host
        self.port = port
        self.collection_name = collection_name
        self.vector_size = vector_size
        
        # Initialize SentenceTransformer model
        print(f"Loading embedding model: {model_name}")
        self.model = SentenceTransformer(model_name)
        
        # Initialize Qdrant client
        try:
            self.client = QdrantClient(
                host=host,
                port=port,
                timeout=10
            )
            # Test connection
            self.client.get_collections()
            print(f"Connected to Qdrant at http://{host}:{port}")
        except Exception as e:
            print(f"Failed to connect to Qdrant: {e}")
            raise
        
        # Ensure collection exists (create if not exists)
        self._ensure_collection_exists()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        # Qdrant client doesn't need explicit closing
        pass
    
    # ========================================================================
    # COLLECTION MANAGEMENT
    # ========================================================================
    
    def collection_exists(self) -> bool:
        """Check if the collection exists."""
        collections = self.client.get_collections().collections
        return any(col.name == self.collection_name for col in collections)
    
    def get_collection_info(self):
        """Get information about the collection."""
        try:
            return self.client.get_collection(self.collection_name)
        except Exception as e:
            print(f"Error getting collection info: {e}")
            return None
    
    def _ensure_collection_exists(self):
        """
        Ensure collection exists. Create if it doesn't exist.
        If it exists, verify vector size compatibility.
        """
        if not self.collection_exists():
            # Create new collection
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(
                    size=self.vector_size,
                    distance=Distance.COSINE
                )
            )
            print(f"Created new collection '{self.collection_name}' with vector size {self.vector_size}")
        else:
            # Collection exists, verify compatibility
            collection_info = self.get_collection_info()
            existing_vector_size = collection_info.config.params.vectors.size
            
            if existing_vector_size != self.vector_size:
                raise ValueError(
                    f"Vector size mismatch: Collection '{self.collection_name}' has size "
                    f"{existing_vector_size}, but model produces size {self.vector_size}. "
                    f"Please use a different collection name or delete the existing collection."
                )
            
            print(f"Using existing collection '{self.collection_name}'")
    
    # ========================================================================
    # EMBEDDING GENERATION
    # ========================================================================
    
    def create_embedding_text(self, product: Dict[str, Any]) -> str:
        """
        Create weighted text for embedding from product fields.
        
        Args:
            product: Product dictionary with name, short_description, description
            
        Returns:
            Combined text string for embedding
        """
        weights = {
            'name': 3,
            'short_description': 1,
            'description': 1
        }
        
        parts = []
        for field, weight in weights.items():
            if field in product and product[field]:
                field_value = str(product[field]).strip()
                if field_value:
                    parts.extend([field_value] * weight)
        
        return ' '.join(parts) if parts else ''
    
    def preprocess_products(self, products: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Preprocess products: create DataFrame and generate embedding text.
        
        Args:
            products: List of product dictionaries
            
        Returns:
            DataFrame with products and text_for_embedding column
        """
        df = pd.DataFrame(products)
        
        # Create embedding text
        df['text_for_embedding'] = df.apply(
            lambda row: self.create_embedding_text(row.to_dict()), 
            axis=1
        )
        
        # Remove rows with empty text_for_embedding
        initial_count = len(df)
        df = df[df['text_for_embedding'] != ''].copy()
        final_count = len(df)
        
        if initial_count != final_count:
            print(f"Filtered out {initial_count - final_count} products with no text content")
        
        print(f"Preprocessed {final_count} products for embedding")
        return df
    
    def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a list of texts.
        
        Args:
            texts: List of text strings
            
        Returns:
            List of embedding vectors
        """
        print(f"Generating embeddings for {len(texts)} products")
        embeddings = self.model.encode(
            texts,
            batch_size=32,
            show_progress_bar=True,
            convert_to_numpy=True
        )
        return embeddings.tolist()
    
    # ========================================================================
    # DATA UPLOAD
    # ========================================================================
    
    def upload_to_qdrant(self, df: pd.DataFrame, embeddings: List[List[float]]):
        """
        Upload product vectors to Qdrant (appends to existing collection).
        
        Args:
            df: DataFrame with product data
            embeddings: List of embedding vectors
        """
        points = []
        
        # Get the starting ID for new points (append to existing)
        start_id = 0
        try:
            collection_info = self.get_collection_info()
            start_id = collection_info.vectors_count
            if start_id > 0:
                print(f"  Appending to existing collection. Starting new vector IDs from: {start_id}")
        except:
            start_id = 0
        
        for idx, (_, row) in enumerate(df.iterrows()):
            # Create payload with product_id and name
            payload = {
                "product_id": str(row.get('id', f'unknown_{idx}'))
            }
            
            if 'name' in row and pd.notna(row['name']):
                payload['name'] = str(row['name'])
            else:
                payload['name'] = row.get('text_for_embedding', '')[:200]
            
            if 'short_description' in row and pd.notna(row['short_description']):
                payload['short_description'] = str(row['short_description'])[:500]
            
            point = PointStruct(
                id=start_id + idx,
                vector=embeddings[idx],
                payload=payload
            )
            points.append(point)
        
        # Upload in batches
        batch_size = 100
        total_points = len(points)
        
        print(f"Uploading {total_points} vectors to Qdrant")
        
        for i in tqdm(range(0, total_points, batch_size), desc="Uploading batches"):
            batch = points[i:i + batch_size]
            self.client.upsert(
                collection_name=self.collection_name,
                points=batch
            )
        
        print(f"Successfully uploaded {total_points} vectors")
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Get collection statistics."""
        collection_info = self.get_collection_info()
        if collection_info:
            return {
                'collection_name': self.collection_name,
                'vectors_count': collection_info.vectors_count,
                'vector_size': collection_info.config.params.vectors.size
            }
        return {}
    
    # ========================================================================
    # MAIN LOADING METHODS
    # ========================================================================
    
    def _load_products(self, products: List[Dict[str, Any]]):
        """
        Core method to load products into Qdrant.
        Always appends to existing collection.
        
        Args:
            products: List of product dictionaries
        """
        print(f"Loading {len(products)} products into Qdrant")
        
        # Preprocess products
        df = self.preprocess_products(products)
        
        if len(df) == 0:
            print("No products to load after preprocessing")
            return
        
        # Generate embeddings
        texts = df['text_for_embedding'].tolist()
        embeddings = self.generate_embeddings(texts)
        
        # Upload to Qdrant (always appends)
        self.upload_to_qdrant(df, embeddings)
        
        # Show statistics
        stats = self.get_collection_stats()
        print(f"\nCollection '{stats['collection_name']}' now contains {stats['vectors_count']} vectors")
    
    def load_products_from_json(self, json_file_path: str):
        """
        Load products from a JSON file.
        
        Args:
            json_file_path: Path to the JSON file containing products
        """
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                products = json.load(f)
            
            print(f"Loaded {len(products)} products from {json_file_path}")
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

def populate_from_data(products: List[Dict[str, Any]]):
    """
    Populate Qdrant database from product data list.
    This function is called from ocr.py's populate_databases method.
    Collection is created if it doesn't exist; data is appended if it does.
    
    Args:
        products: List of product dictionaries with id, name, short_description, description
    """
    # Get configuration from environment or use defaults
    qdrant_host = os.environ.get('QDRANT_HOST', 'localhost')
    qdrant_port = int(os.environ.get('QDRANT_PORT', '6334'))
    collection_name = os.environ.get('QDRANT_COLLECTION', 'products')
    model_name = os.environ.get('EMBEDDING_MODEL', 'all-MiniLM-L6-v2')
    
    with QdrantProductLoader(
        host=qdrant_host,
        port=qdrant_port,
        collection_name=collection_name,
        model_name=model_name
    ) as loader:
        loader.load_products_from_data(products)
        
        # Show final statistics
        stats = loader.get_collection_stats()
        print(f"Qdrant population complete!")
        print(f"  Collection: {stats['collection_name']}")
        print(f"  Total vectors: {stats['vectors_count']}")


def main():
    """Main function for standalone execution."""
    # Configuration
    QDRANT_HOST = os.environ.get('QDRANT_HOST', 'localhost')
    QDRANT_PORT = int(os.environ.get('QDRANT_PORT', '6334'))
    COLLECTION_NAME = os.environ.get('QDRANT_COLLECTION', 'products')
    MODEL_NAME = os.environ.get('EMBEDDING_MODEL', 'all-MiniLM-L6-v2')
    DATA_FILE = "final_data_qdrant.json"
    
    with QdrantProductLoader(
        host=QDRANT_HOST,
        port=QDRANT_PORT,
        collection_name=COLLECTION_NAME,
        model_name=MODEL_NAME
    ) as loader:
        try:
            loader.load_products_from_json(DATA_FILE)
            
            # Show final statistics
            stats = loader.get_collection_stats()
            print(f"\nPopulation complete!")
            print(f"  Collection: {stats['collection_name']}")
            print(f"  Total vectors: {stats['vectors_count']}")
            
        except Exception as e:
            print(f"\nError: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()