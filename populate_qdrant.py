import json
import sys
from typing import List, Dict, Any
import pandas as pd
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from tqdm import tqdm

def load_product_data(file_path):

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

def create_embedding_text(row):

    weights = {
        'name': 3,              
        'short_description': 1,  
        'description': 1        
    }
    parts = []
    
    for field, weight in weights.items():
        if field in row and pd.notna(row.get(field)):
            field_value = str(row[field]).strip()
            
            if field_value:
                parts.extend([field_value] * weight)
    
    return ' '.join(parts) if parts else ''

def preprocess_products(df):
    
    df['text_for_embedding'] = df.apply(create_embedding_text, axis=1)
    
    # Remove rows with empty text_for_embedding
    initial_count = len(df)
    df = df[df['text_for_embedding'] != ''].copy()
    final_count = len(df)
    
    if initial_count != final_count:
        print(f"Filtered out {initial_count - final_count} products with no text content")
    
    print(f"Preprocessed {final_count} products for embedding")
    return df

def generate_embeddings(texts, model):

    print(f"Generating embeddings for {len(texts)} products")
    embeddings = model.encode(
        texts,
        batch_size=32,
        show_progress_bar=True,
        convert_to_numpy=True
    )
    return embeddings.tolist()

def initialize_qdrant_collection(client, collection_name, vector_size):
    try:
        # Check if collection exists
        collections = client.get_collections().collections
        exists = any(col.name == collection_name for col in collections)
        
        if exists:
            # Get collection info
            collection_info = client.get_collection(collection_name)

            print(f"\nCollection '{collection_name}' already exists")

            print("type 1 to delete existing collection and create new one")
            print("type 2 to keep existing collection and do nothing to the current collection")
    
            choice = input().strip()

            if choice == '1':
                
                print(f"Deleting collection '{collection_name}'")
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
                print(f"Created new collection '{collection_name}' with vector size {vector_size}")
                return True
            
            elif choice == '2':
                # Check vector size compatibility
                existing_vector_size = collection_info.config.params.vectors.size
                if existing_vector_size != vector_size:
                    print(f"Error: Vector size mismatch")
                    print(f"Existing: {existing_vector_size}, Required: {vector_size}")
                    sys.exit(1)

                print(f"Using existing collection")
                
                return False
            
        else:
            # Collection doesn't exist, create new one
            client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=vector_size,
                    distance=Distance.COSINE
                )
            )
            print(f"Created new collection '{collection_name}' with vector size {vector_size}")
            return True
        
    except Exception as e:
        print(f"Error initializing collection: {e}")
        sys.exit(1)

def upload_to_qdrant(client, collection_name, df, embeddings, is_new_collection):

    points = []
    
    # Get the starting ID for new points if appending to existing collection
    start_id = 0
    if not is_new_collection:
        try:
            collection_info = client.get_collection(collection_name)
            start_id = collection_info.vectors_count
            print(f"  Starting new vector IDs from: {start_id}")
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
    
    batch_size = 100
    total_points = len(points)
    
    print(f"Uploading {total_points} vectors to Qdrant")
    
    for i in tqdm(range(0, total_points, batch_size)):
        batch = points[i:i + batch_size]
        client.upsert(
            collection_name=collection_name,
            points=batch
        )
    
    print(f"Successfully uploaded vectors")

def main():
    # Configuration
    QDRANT_HOST = "localhost"
    QDRANT_PORT = 6334
    COLLECTION_NAME = "products"
    MODEL_NAME = "all-MiniLM-L6-v2"
    VECTOR_SIZE = 384 
    DATA_FILE = "final_data_qdrant.json"
    
    # Initialize sentence transformer
    model = SentenceTransformer(MODEL_NAME)
    
    # Initialize Qdrant client
    try:
        client = QdrantClient(
            host=QDRANT_HOST,
            port=QDRANT_PORT,
            timeout=10
        )
        # Test connection
        client.get_collections()
        print(f"Connected to Qdrant at http://{QDRANT_HOST}:{QDRANT_PORT}")
    except Exception as e:
        print(f"Failed to connect to Qdrant: {e}")
        sys.exit(1)
    
    # Load and preprocess data
    df = load_product_data(DATA_FILE)
    df = preprocess_products(df)
    
    # Initialize collection
    is_new_collection = initialize_qdrant_collection(client, COLLECTION_NAME, VECTOR_SIZE)
    
    # Generate embeddings
    texts = df['text_for_embedding'].tolist()
    embeddings = generate_embeddings(texts, model)
    
    # Upload to Qdrant
    upload_to_qdrant(client, COLLECTION_NAME, df, embeddings, is_new_collection)
    
    # # to check some stats
    # collection_info = client.get_collection(COLLECTION_NAME)
    # print(f"\nPopulation complete!")
    # print(f"  Collection: {COLLECTION_NAME}")
    # print(f"  Total vectors: {collection_info.vectors_count}")

if __name__ == "__main__":
    main()