"""
Simple script to populate Neo4j and Qdrant databases from products.json file.
"""
import json
import sys
from pathlib import Path
from typing import List, Dict, Any

from populate_neo4j_latest import populate_from_data as populate_neo4j
from populate_qdrant import populate_from_data as populate_qdrant


def load_products_json(file_path: str = "products.json") -> List[Dict[str, Any]]:

    path = Path(file_path)
    
    if not path.exists():
        print(f"Error: File '{file_path}' not found in current directory")
        sys.exit(1)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            products = json.load(f)
        
        print(f"✓ Loaded {len(products)} products from {file_path}")
        return products
    
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in '{file_path}': {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading file: {e}")
        sys.exit(1)

def main():
    """Main function to populate both databases."""
    print("=" * 70)
    print("DATABASE POPULATION FROM products.json")
    print("=" * 70)
    print()
    
    # Step 1: Load products from JSON
    products_neo4j = load_products_json("final_data_neo4j.json")
    print()

    products_qdrant = load_products_json("final_data_qdrant.json")
    print()
    
    # Step 4: Populate Neo4j
    print("Step 4: Populating Neo4j Database")
    print("-" * 70)
    try:
        populate_neo4j(products_neo4j)
        print("✓ Neo4j population completed successfully")
    except Exception as e:
        print(f"✗ Error populating Neo4j: {e}")
        print("Continuing with Qdrant...")
    print()
    
    # Step 5: Populate Qdrant
    print("Step 5: Populating Qdrant Database")
    print("-" * 70)
    try:
        populate_qdrant(products_qdrant)
        print("✓ Qdrant population completed successfully")
    except Exception as e:
        print(f"✗ Error populating Qdrant: {e}")
    print()
    
    # Summary
    print("=" * 70)
    print("DATABASE POPULATION COMPLETE!")
    print("=" * 70)


if __name__ == "__main__":
    main()