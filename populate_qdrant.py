import json
import sys
from typing import List, Dict, Any
import pandas as pd
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from tqdm import tqdm


def load_product_data(file_path: str) -> pd.DataFrame:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        df = pd.DataFrame(data)
        print(f"Loaded {len(df)} products from {file_path}")
        return df
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in file '{file_path}': {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading data: {e}")
        sys.exit(1)

def preprocess_products(df: pd.DataFrame) -> pd.DataFrame:

    def create_embedding_text(row):
        parts = []
        
        # Add name if exists
        if pd.notna(row.get('name')) and str(row['name']).strip():
            parts.append(str(row['name']).strip())
        
        # Add short_description if exists
        if pd.notna(row.get('short_description')) and str(row['short_description']).strip():
            parts.append(str(row['short_description']).strip())
        
        # Add description if exists
        if pd.notna(row.get('description')) and str(row['description']).strip():
            parts.append(str(row['description']).strip())
        
        # Join with space separator
        return ' '.join(parts) if parts else ''
    
    df['text_for_embedding'] = df.apply(create_embedding_text, axis=1)
    
    # Remove rows with empty text_for_embedding
    initial_count = len(df)
    df = df[df['text_for_embedding'] != ''].copy()
    final_count = len(df)
    
    if initial_count != final_count:
        print(f"Filtered out {initial_count - final_count} products with no text content")
    
    print(f"Preprocessed {final_count} products for embedding")
    return df

def generate_embeddings(texts: List[str], model: SentenceTransformer) -> List[List[float]]:

    print(f"Generating embeddings for {len(texts)} products...")
    embeddings = model.encode(
        texts,
        batch_size=32,
        show_progress_bar=True,
        convert_to_numpy=True
    )
    return embeddings.tolist()

def initialize_qdrant_collection(client: QdrantClient, collection_name: str, vector_size: int):

    try:
        # Check if collection exists
        collections = client.get_collections().collections
        exists = any(col.name == collection_name for col in collections)
        
        if exists:
            print(f"Collection '{collection_name}' exists. Deleting...")
            client.delete_collection(collection_name)
            print(f"Deleted existing collection '{collection_name}'")
        
        # Create new collection
        client.recreate_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(
                size=vector_size,
                distance=Distance.COSINE
            )
        )
        print(f"Created collection '{collection_name}' with vector size {vector_size}")
        
    except Exception as e:
        print(f"✗ Error initializing collection: {e}")
        sys.exit(1)

def upload_to_qdrant(
    client: QdrantClient,
    collection_name: str,
    df: pd.DataFrame,
    embeddings: List[List[float]]
):
    points = []
    
    for idx, (_, row) in enumerate(df.iterrows()):
        # Create payload with product_id
        payload = {
            "product_id": str(row.get('id', f'unknown_{idx}'))
        }
        
        # Optionally add other metadata
        if 'name' in row and pd.notna(row['name']):
            payload['name'] = str(row['name'])
        
        # Create point
        point = PointStruct(
            id=idx,
            vector=embeddings[idx],
            payload=payload
        )
        points.append(point)
    
    # Upload in batches
    batch_size = 100
    total_points = len(points)
    
    for i in tqdm(range(0, total_points, batch_size)):
        batch = points[i:i + batch_size]
        client.upsert(
            collection_name=collection_name,
            points=batch
        )
    
    print(f"Successfully uploaded {total_points} vectors to collection '{collection_name}'")

def main():
    
    # Configuration
    QDRANT_HOST = "localhost"
    QDRANT_PORT = 6334
    COLLECTION_NAME = "products"
    MODEL_NAME = "all-MiniLM-L6-v2"
    VECTOR_SIZE = 384  # Size for all-MiniLM-L6-v2
    DATA_FILE = "final_data_qdrant.json"
    
    model = SentenceTransformer(MODEL_NAME)
  
    # Initialize Qdrant client
    try:
        client = QdrantClient(
            host=QDRANT_HOST,
            port=QDRANT_PORT,
            timeout=30
        )
        # Test connection
        client.get_collections()
        print(f"\nConnected to Qdrant at http://{QDRANT_HOST}:{QDRANT_PORT}")
    except Exception as e:
        print(f"\nFailed to connect to Qdrant: {e}")
        sys.exit(1)
    
    # Load and preprocess data
    df = load_product_data(DATA_FILE)
    df = preprocess_products(df)
    
    # Initialize collection
    initialize_qdrant_collection(client, COLLECTION_NAME, VECTOR_SIZE)
    
    # Generate embeddings
    texts = df['text_for_embedding'].tolist()
    embeddings = generate_embeddings(texts, model)
    
    # Upload to Qdrant
    upload_to_qdrant(client, COLLECTION_NAME, df, embeddings)

if __name__ == "__main__":
    main()