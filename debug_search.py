#!/usr/bin/env python3
"""
debug_search.py - Diagnostic script to troubleshoot search issues
"""

import os
from neo4j import GraphDatabase


def run_diagnostics():
    """Run diagnostic checks on the Neo4j database"""
    
    # Connection details
    neo4j_uri = os.environ.get('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_user = os.environ.get('NEO4J_USER', 'neo4j')
    neo4j_password = os.environ.get('NEO4J_PASSWORD', 'password')
    
    print("=" * 80)
    print("Neo4j Search Diagnostics")
    print("=" * 80)
    print(f"Connecting to: {neo4j_uri}")
    
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    
    try:
        with driver.session() as session:
            # # 1. Check product count
            # print("\n1. Checking product count...")
            # result = session.run("MATCH (p:Product) RETURN count(p) as count")
            # count = result.single()['count']
            # print(f"   ✓ Found {count} products in database")
            
            # # 2. Check sample products
            # print("\n2. Sample products in database:")
            # result = session.run("""
            #     MATCH (p:Product) 
            #     RETURN p.id, p.name, p.short_description 
            #     LIMIT 5
            # """)
            # for record in result:
            #     print(f"   - ID: {record['p.id']}")
            #     print(f"     Name: {record['p.name']}")
            #     desc = record['p.short_description'] or "(no description)"
            #     if len(desc) > 50:
            #         desc = desc[:47] + "..."
            #     print(f"     Description: {desc}")
            
            # 3. Check for products with "CX-112" in name
            print("\n3. Looking for products containing 'CX-112'...")
            result = session.run("""
                MATCH (p:Product)
                WHERE p.name CONTAINS 'CX-112'
                RETURN p.id, p.name
                LIMIT 5
            """)
            found = False
            for record in result:
                found = True
                print(f"   ✓ Found: {record['p.name']} (ID: {record['p.id']})")
            if not found:
                print("   ✗ No products found containing 'CX-112'")
                
                # Try case-insensitive
                print("\n   Trying case-insensitive search...")
                result = session.run("""
                    MATCH (p:Product)
                    WHERE toLower(p.name) CONTAINS toLower('CX-112')
                    RETURN p.id, p.name
                    LIMIT 5
                """)
                for record in result:
                    found = True
                    print(f"   ✓ Found (case-insensitive): {record['p.name']} (ID: {record['p.id']})")
                    
                if not found:
                    print("   ✗ Still no products found")
            
            # 4. Check indexes
            print("\n4. Checking indexes...")
            result = session.run("SHOW INDEXES")
            fulltext_found = False
            for record in result:
                if 'product_search' in str(record):
                    fulltext_found = True
                    print(f"   ✓ Full-text index 'product_search' exists")
                    print(f"     State: {record.get('state', 'unknown')}")
            
            if not fulltext_found:
                print("   ✗ Full-text index 'product_search' NOT found!")
                print("   Creating index now...")
                
                # Try to create the index
                try:
                    session.run("DROP INDEX product_search IF EXISTS")
                except:
                    pass
                    
                session.run("""
                    CREATE FULLTEXT INDEX product_search 
                    FOR (p:Product)
                    ON EACH [p.name, p.short_description]
                """)
                print("   ✓ Index created")
            
            # 5. Test full-text search
            print("\n5. Testing full-text search...")
            test_queries = [
                "CX-112"
            ]
            
            for test_query in test_queries:
                print(f"\n   Testing query: '{test_query}'")
                
                # Try full-text search
                try:
                    result = session.run("""
                        CALL db.index.fulltext.queryNodes('product_search', $query)
                        YIELD node, score
                        RETURN node.name as name, score
                        ORDER BY score DESC
                        LIMIT 3
                    """, query=test_query)
                    
                    found_any = False
                    for record in result:
                        found_any = True
                        print(f"     ✓ {record['name'][:50]}... (score: {record['score']:.3f})")
                    
                    if not found_any:
                        print(f"     ✗ No results from full-text search")
                        
                except Exception as e:
                    print(f"     ✗ Full-text search failed: {e}")
                    
                # Try CONTAINS as fallback
                print(f"   Testing CONTAINS for '{test_query}':")
                result = session.run("""
                    MATCH (p:Product)
                    WHERE toLower(p.name) CONTAINS toLower($query)
                    RETURN p.name as name
                    LIMIT 3
                """, query=test_query)
                
                found_any = False
                for record in result:
                    found_any = True
                    print(f"     ✓ {record['name'][:50]}...")
                
                if not found_any:
                    print(f"     ✗ No results from CONTAINS search")
            
            # 6. Check for empty descriptions
            print("\n6. Checking for products with empty descriptions...")
            result = session.run("""
                MATCH (p:Product)
                WHERE p.short_description IS NULL OR p.short_description = ''
                RETURN count(p) as count
            """)
            empty_count = result.single()['count']
            print(f"   Products with empty descriptions: {empty_count}")
            
            print("\n" + "=" * 80)
            print("Diagnostics complete!")
            print("=" * 80)
            
    except Exception as e:
        print(f"\nError during diagnostics: {e}")
    finally:
        driver.close()


if __name__ == "__main__":
    run_diagnostics()