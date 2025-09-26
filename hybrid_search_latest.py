import os
import sys
import re
from typing import List, Dict, Set
from difflib import SequenceMatcher
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient

# Optional imports for enhanced fuzzy matching
try:
    import jellyfish
    JELLYFISH_AVAILABLE = True
except ImportError:
    print("Warning: jellyfish not available. Install with 'pip install jellyfish' for better fuzzy matching")
    JELLYFISH_AVAILABLE = False

try:
    import nltk
    from nltk.stem import PorterStemmer
    from nltk.corpus import stopwords
    # Download required NLTK data
    try:
        nltk.data.find('tokenizers/punkt')
    except LookupError:
        nltk.download('punkt', quiet=True)
    try:
        nltk.data.find('corpora/stopwords')
    except LookupError:
        nltk.download('stopwords', quiet=True)
    NLTK_AVAILABLE = True
except ImportError:
    print("Warning: nltk not available. Install with 'pip install nltk' for better text processing")
    NLTK_AVAILABLE = False


class FuzzySearchEnhancer:
    """Handles all fuzzy search enhancements"""
    
    def __init__(self):
        # Initialize NLTK components if available
        if NLTK_AVAILABLE:
            try:
                self.stemmer = PorterStemmer()
                self.stop_words = set(stopwords.words('english'))
            except Exception as e:
                print(f"Warning: NLTK setup failed: {e}")
                self.stemmer = None
                self.stop_words = self._get_basic_stopwords()
        else:
            self.stemmer = None
            self.stop_words = self._get_basic_stopwords()
        
        # Extended misspellings dictionary
        self.common_misspellings = {
            'labtop': 'laptop', 'laptpo': 'laptop', 'laptp': 'laptop',
            'wireles': 'wireless', 'wirless': 'wireless', 'wirelss': 'wireless',
            'computor': 'computer', 'compter': 'computer', 'computr': 'computer',
            'keboard': 'keyboard', 'keybaord': 'keyboard', 'keybord': 'keyboard',
            'accesory': 'accessory', 'accessry': 'accessory', 'accesorry': 'accessory',
            'conector': 'connector', 'connectr': 'connector', 'connctor': 'connector',
            'reciver': 'receiver', 'reciever': 'receiver', 'recever': 'receiver',
            'transmiter': 'transmitter', 'transmiter': 'transmitter', 'transmiter': 'transmitter',
            'elecric': 'electric', 'elctric': 'electric', 'electirc': 'electric',
            'baterry': 'battery', 'batery': 'battery', 'battry': 'battery',
            'chargr': 'charger', 'charger': 'charger', 'chager': 'charger',
            'adaptr': 'adapter', 'adaptor': 'adapter', 'adpater': 'adapter',
            'cabl': 'cable', 'cabel': 'cable', 'calbe': 'cable',
            'devic': 'device', 'deivce': 'device', 'divice': 'device',
            'netwrk': 'network', 'netowrk': 'network', 'netowork': 'network',
            'memorry': 'memory', 'memroy': 'memory', 'memeory': 'memory',
            'storag': 'storage', 'storeage': 'storage', 'storge': 'storage'
        }
    
    def _get_basic_stopwords(self):
        """Basic stopwords list when NLTK is not available"""
        return {'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from', 
                'the', 'to', 'of', 'in', 'is', 'it', 'on', 'that', 'this', 'with',
                'or', 'but', 'not', 'can', 'will', 'has', 'have', 'had', 'was', 'were'}
    
    def simple_stem(self, word: str) -> str:
        """Basic stemming when NLTK is not available"""
        word = word.lower()
        if len(word) <= 3:
            return word
        
        # Handle common suffixes
        suffixes = [
            ('ing', 3), ('ed', 2), ('er', 2), ('est', 3), 
            ('ly', 2), ('tion', 4), ('ness', 4), ('ment', 4),
            ('able', 4), ('ible', 4), ('ful', 3), ('less', 4)
        ]
        
        for suffix, min_length in suffixes:
            if len(word) > min_length and word.endswith(suffix):
                return word[:-len(suffix)]
        
        return word
    
    def get_stem(self, word: str) -> str:
        """Get stem of word using available stemmer"""
        if self.stemmer:
            return self.stemmer.stem(word)
        return self.simple_stem(word)
    
    def fuzzy_similarity(self, s1: str, s2: str) -> float:
        """Calculate fuzzy similarity between two strings"""
        if s1 == s2:
            return 1.0
        
        s1_lower, s2_lower = s1.lower(), s2.lower()
        
        # Use jellyfish if available for better fuzzy matching
        if JELLYFISH_AVAILABLE:
            jaro_score = jellyfish.jaro_winkler_similarity(s1_lower, s2_lower)
            seq_score = SequenceMatcher(None, s1_lower, s2_lower).ratio()
            return max(jaro_score, seq_score)
        else:
            # Fallback to difflib
            seq_score = SequenceMatcher(None, s1_lower, s2_lower).ratio()
            
            # Boost score for substring matches
            if s1_lower in s2_lower or s2_lower in s1_lower:
                return max(seq_score, 0.7)
            
            return seq_score
    
    def get_phonetic_code(self, word: str) -> str:
        """Get phonetic code for word"""
        if JELLYFISH_AVAILABLE:
            return jellyfish.soundex(word)
        else:
            # Simple fallback phonetic encoding
            word = word.lower()
            # Remove vowels except first letter, keep consonants
            if not word:
                return ""
            
            result = word[0]
            for char in word[1:]:
                if char not in 'aeiou':
                    result += char
            
            return result[:4].ljust(4, '0')  # Pad to 4 chars like Soundex
    
    def normalize_search_input_enhanced(self, query: str) -> Dict[str, Set[str]]:
        """Enhanced normalization with comprehensive fuzzy matching"""
        results = {
            'original_terms': set(),
            'corrected_terms': set(),
            'stemmed_terms': set(),
            'phonetic_terms': set(),
            'partial_terms': set(),
            'product_codes': set(),
            'all_terms': set(),  # Combined fuzzy terms (excludes product codes)
            'is_product_code_query': False  # Flag to indicate if query contains product codes
        }
        
        # Store original query and basic forms
        query_clean = query.lower().strip()
        results['original_terms'].add(query_clean)
        results['original_terms'].add(query.strip())  # Keep original case too
        
        # Handle product codes with enhanced variations (separate from fuzzy matching)
        if re.match(r'.*[A-Z0-9]+-[A-Z0-9]+.*', query.upper()):
            results['is_product_code_query'] = True
            code_variations = self._generate_enhanced_code_variations(query)
            results['product_codes'].update(code_variations)
            # For product code queries, also add the original query as-is
            results['original_terms'].add(query.strip())
            return results  # Early return - skip fuzzy processing for product codes
        
        # Process individual words
        words = re.split(r'[\s\-_,;:.()]+', query_clean)
        for word in words:
            word = word.strip()
            if len(word) <= 1 or word in self.stop_words:
                continue
            
            # Original word
            results['original_terms'].add(word)
            
            # Check for misspelling corrections
            if word in self.common_misspellings:
                corrected = self.common_misspellings[word]
                results['corrected_terms'].add(corrected)
                results['stemmed_terms'].add(self.get_stem(corrected))
            
            # Stemmed version
            stemmed = self.get_stem(word)
            results['stemmed_terms'].add(stemmed)
            
            # Phonetic version
            phonetic = self.get_phonetic_code(word)
            results['phonetic_terms'].add(phonetic)
            
            # Partial terms for compound word matching
            if len(word) > 4:
                results['partial_terms'].add(word[:3])
                results['partial_terms'].add(word[-3:])
                if len(word) > 6:
                    results['partial_terms'].add(word[:4])
                    results['partial_terms'].add(word[-4:])
                    results['partial_terms'].add(word[1:-1])  # Middle part
        
        # Combine fuzzy terms for unified searching (exclude product_codes)
        for key, term_set in results.items():
            if isinstance(term_set, set) and key not in ['all_terms', 'product_codes'] and key != 'is_product_code_query':
                results['all_terms'].update(term_set)
        
        return results
    
    def _generate_enhanced_code_variations(self, code: str) -> Set[str]:
        """Generate comprehensive product code variations"""
        variations = set()
        
        # Basic variations
        variations.update({code.upper(), code.lower(), code})
        
        # Hyphen variations
        no_hyphen = code.replace('-', '')
        variations.update({no_hyphen.upper(), no_hyphen.lower()})
        
        # Space variations
        space_version = code.replace('-', ' ')
        variations.update({space_version.upper(), space_version.lower()})
        
        # Underscore variations
        underscore_version = code.replace('-', '_')
        variations.update({underscore_version.upper(), underscore_version.lower()})
        
        # Dot variations
        dot_version = code.replace('-', '.')
        variations.update({dot_version.upper(), dot_version.lower()})
        
        # Mixed case variations for readability
        if '-' in code:
            parts = code.split('-')
            if len(parts) >= 2:
                # First part upper, rest lower
                mixed1 = parts[0].upper() + '-' + '-'.join(p.lower() for p in parts[1:])
                variations.add(mixed1)
                
                # Alternating case
                mixed2 = '-'.join(parts[i].upper() if i % 2 == 0 else parts[i].lower() 
                                for i in range(len(parts)))
                variations.add(mixed2)
        
        return variations


class EnhancedHybridSearch:
    
    def __init__(self, 
                 neo4j_uri="bolt://localhost:7687",
                 neo4j_user="neo4j", 
                 neo4j_password="password",
                 qdrant_host="localhost",
                 qdrant_port=6334):
        
        # Initialize fuzzy search enhancer
        self.fuzzy_enhancer = FuzzySearchEnhancer()
        
        # Neo4j connection
        try:
            self.neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
            with self.neo4j_driver.session() as session:
                result = session.run("MATCH (p:Product) RETURN count(p) as count")
                count = result.single()['count']
                print(f"Connected to Neo4j. Found {count} products")
        except Exception as e:
            print(f"Failed to connect to Neo4j: {e}")
            sys.exit(1)
        
        # Qdrant connection
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

    def _search_product_codes_exact(self, product_codes: Set[str], limit: int) -> Dict:
        """Exact search for product codes without fuzzy matching"""
        neo4j_results = {}
        product_codes_list = list(product_codes)

        with self.neo4j_driver.session() as session:
            # Exact product code matching only
            try:
                result = session.run("""
                    MATCH (p:Product)
                    WHERE ANY(code IN $product_codes WHERE code IN p.search_terms_list)
                    WITH p, SIZE([code IN $product_codes WHERE code IN p.search_terms_list]) as exact_matches
                    WHERE exact_matches > 0
                    RETURN p.id as product_id, p.name as product_name,
                           p.short_description as description, exact_matches as score
                    ORDER BY score DESC
                    LIMIT $limit
                """, product_codes=product_codes_list, limit=limit)

                for record in result:
                    pid = record['product_id']
                    neo4j_results[pid] = {
                        'id': pid,
                        'name': record['product_name'],
                        'score': float(record['score']) * 5.0  # High weight for exact code matches
                    }
                print(f"Product code search: Found {len(neo4j_results)} exact matches")

            except Exception as e:
                print(f"Neo4j product code search error: {e}")

            # Also try full-text search for product codes
            try:
                for code in product_codes_list:
                    result = session.run("""
                        CALL db.index.fulltext.queryNodes('product_search', $search_term)
                        YIELD node, score
                        WHERE score > 0.8  // High threshold for product codes
                        RETURN node.id as product_id, node.name as product_name, score
                        ORDER BY score DESC
                        LIMIT $limit
                    """, search_term=code, limit=limit)

                    for record in result:
                        pid = record['product_id']
                        if pid not in neo4j_results:  # Don't overwrite exact matches
                            neo4j_results[pid] = {
                                'id': pid,
                                'name': record['product_name'],
                                'score': float(record['score']) * 3.0  # Lower weight than exact matches
                            }
            except Exception as e:
                print(f"Neo4j product code fulltext search error: {e}")

        return neo4j_results
    
    def search_neo4j_enhanced(self, query, limit):
        """Enhanced Neo4j search with fuzzy matching capabilities"""
        # Get enhanced search terms
        search_terms_dict = self.fuzzy_enhancer.normalize_search_input_enhanced(query)

        neo4j_results = {}

        # Handle product code queries separately (exact matching only)
        if search_terms_dict['is_product_code_query']:
            return self._search_product_codes_exact(search_terms_dict['product_codes'], limit)

        # Use fuzzy terms for non-product-code queries
        all_search_terms = list(search_terms_dict['all_terms'])
        
        with self.neo4j_driver.session() as session:
            # 1. Enhanced pre-computed terms search with fuzzy scoring
            try:
                result = session.run("""
                    MATCH (p:Product)
                    WHERE ANY(term IN $search_terms WHERE 
                        term IN p.search_terms_list OR
                        ANY(stored_term IN p.search_terms_list WHERE 
                            stored_term CONTAINS term OR term CONTAINS stored_term))
                    
                    WITH p,
                        SIZE([term IN $search_terms WHERE term IN p.search_terms_list]) as exact_matches,
                        SIZE([term IN $search_terms WHERE 
                              ANY(stored_term IN p.search_terms_list WHERE 
                                  stored_term CONTAINS term AND stored_term <> term)]) as partial_matches,
                        SIZE([term IN $search_terms WHERE 
                              ANY(stored_term IN p.search_terms_list WHERE 
                                  term CONTAINS stored_term AND stored_term <> term)]) as substring_matches
                    
                    WITH p, (exact_matches * 5.0 + partial_matches * 2.5 + substring_matches * 1.5) as fuzzy_score
                    WHERE fuzzy_score > 0
                    
                    RETURN p.id as product_id, p.name as product_name, 
                           p.short_description as description, fuzzy_score as score
                    ORDER BY score DESC
                    LIMIT $limit
                """, search_terms=all_search_terms, limit=limit)
                
                for record in result:
                    pid = record['product_id']
                    neo4j_results[pid] = {
                        'id': pid,
                        'name': record['product_name'],
                        'score': float(record['score']) * 2.0  # Weight factor
                    }
            except Exception as e:
                print(f"Neo4j enhanced precomputed search error: {e}")
            
            # 2. Enhanced full-text search
            try:
                # Search with original query
                result = session.run("""
                    CALL db.index.fulltext.queryNodes('product_search', $search_term)
                    YIELD node, score
                    RETURN node.id as product_id, node.name as product_name, score
                    ORDER BY score DESC
                    LIMIT $limit
                """, search_term=query, limit=limit)
                
                for record in result:
                    pid = record['product_id']
                    score_boost = float(record['score']) * 1.8
                    if pid in neo4j_results:
                        neo4j_results[pid]['score'] += score_boost
                    else:
                        neo4j_results[pid] = {
                            'id': pid,
                            'name': record['product_name'],
                            'score': score_boost
                        }
                
                # Also search with corrected terms if any corrections were made
                if search_terms_dict['corrected_terms']:
                    corrected_query = ' '.join(search_terms_dict['corrected_terms'])
                    result = session.run("""
                        CALL db.index.fulltext.queryNodes('product_search', $search_term)
                        YIELD node, score
                        RETURN node.id as product_id, node.name as product_name, score
                        ORDER BY score DESC
                        LIMIT $limit
                    """, search_term=corrected_query, limit=limit)
                    
                    for record in result:
                        pid = record['product_id']
                        score_boost = float(record['score']) * 1.5  # Slightly lower for corrections
                        if pid in neo4j_results:
                            neo4j_results[pid]['score'] += score_boost
                        else:
                            neo4j_results[pid] = {
                                'id': pid,
                                'name': record['product_name'],
                                'score': score_boost
                            }
                            
            except Exception as e:
                print(f"Neo4j enhanced fulltext search error: {e}")
            
            # 3. Enhanced attribute search with fuzzy matching
            try:
                result = session.run("""
                    MATCH (p:Product)-[:HAS_ATTRIBUTE]->(a:Attribute)
                    WHERE ANY(term IN $search_terms WHERE 
                             toLower(a.key) CONTAINS term OR 
                             toLower(a.value) CONTAINS term OR
                             term CONTAINS toLower(a.key) OR
                             term CONTAINS toLower(a.value))
                    WITH p, COUNT(DISTINCT a) as matching_attrs
                    RETURN p.id as product_id, p.name as product_name, matching_attrs as score
                    ORDER BY score DESC
                    LIMIT $limit
                """, search_terms=all_search_terms, limit=limit)
                
                for record in result:
                    pid = record['product_id']
                    score_boost = float(record['score']) * 1.3
                    if pid in neo4j_results:
                        neo4j_results[pid]['score'] += score_boost
                    else:
                        neo4j_results[pid] = {
                            'id': pid,
                            'name': record['product_name'],
                            'score': score_boost
                        }
            except Exception as e:
                print(f"Neo4j enhanced attribute search error: {e}")
        
        return neo4j_results
    
    def search_qdrant(self, query, limit):
        """Standard Qdrant search (unchanged - already handles semantic similarity well)"""
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
    
    def normalize_scores(self, scores_dict, score_key='score'):
        """Normalize scores to 0-1 range"""
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
    
    def apply_fuzzy_post_processing(self, combined_results: Dict, original_query: str) -> Dict:
        """Apply additional fuzzy matching to boost relevant results"""
        query_lower = original_query.lower()
        
        for pid, result in combined_results.items():
            name = result.get('name', '').lower()
            
            # Calculate name similarity bonus
            name_similarity = self.fuzzy_enhancer.fuzzy_similarity(query_lower, name)
            
            # Apply fuzzy bonus for high similarity
            if name_similarity > 0.6:
                fuzzy_bonus = name_similarity * 0.3  # 30% bonus weight
                result['fuzzy_bonus'] = fuzzy_bonus
                result['combined_score'] = result.get('combined_score', 0) + fuzzy_bonus
            else:
                result['fuzzy_bonus'] = 0.0
            
            # Word-level matching bonus
            query_words = set(re.split(r'[\s\-_,;:.()]+', query_lower))
            name_words = set(re.split(r'[\s\-_,;:.()]+', name))
            
            # Remove stop words
            query_words = query_words - self.fuzzy_enhancer.stop_words
            name_words = name_words - self.fuzzy_enhancer.stop_words
            
            # Calculate word overlap
            if query_words and name_words:
                word_overlap = len(query_words.intersection(name_words)) / len(query_words)
                if word_overlap > 0:
                    word_bonus = word_overlap * 0.2  # 20% bonus weight
                    result['combined_score'] = result.get('combined_score', 0) + word_bonus
        
        return combined_results
    
    def enhanced_hybrid_search(self, query, neo4j_weight=0.6, qdrant_weight=0.4):
        """Main enhanced hybrid search method"""
        if not query.strip():
            return []

        # Check if this is a product code query first
        search_terms_dict = self.fuzzy_enhancer.normalize_search_input_enhanced(query)
        if search_terms_dict['is_product_code_query']:
            print(f"\nProduct Code Query Detected - Using Exact Matching")
            neo4j_results = self.search_neo4j_enhanced(query, limit=25)
            print(f"Found {len(neo4j_results)} exact product matches")

            # For product codes, skip Qdrant and return exact matches
            if neo4j_results:
                neo4j_results = self.normalize_scores(neo4j_results)
                sorted_results = sorted(
                    [{'id': data['id'], 'name': data['name'], 'neo4j_score': data['normalized_score'],
                      'qdrant_score': 0, 'combined_score': data['normalized_score']}
                     for data in neo4j_results.values()],
                    key=lambda x: x['combined_score'], reverse=True
                )[:15]
                return sorted_results
            else:
                return []

        print(f"\nEnhanced Querying Neo4j:")
        neo4j_results = self.search_neo4j_enhanced(query, limit=25)
        print(f" Found {len(neo4j_results)} products")

        print("Querying Qdrant:")
        qdrant_results = self.search_qdrant(query, limit=25)
        print(f" Found {len(qdrant_results)} products")
        
        # Normalize scores
        if neo4j_results:
            neo4j_results = self.normalize_scores(neo4j_results)
        if qdrant_results:
            qdrant_results = self.normalize_scores(qdrant_results)
        
        # Combine results
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
                combined_results[pid]['qdrant_score'] = data['normalized_score']
                combined_results[pid]['combined_score'] = (
                    neo4j_weight * combined_results[pid]['neo4j_score'] +
                    qdrant_weight * data['normalized_score']
                )
            else:
                combined_results[pid] = {
                    'id': pid,
                    'name': data['name'],
                    'neo4j_score': 0,
                    'qdrant_score': data['normalized_score'],
                    'combined_score': qdrant_weight * data['normalized_score']
                }
        
        # Apply fuzzy post-processing
        combined_results = self.apply_fuzzy_post_processing(combined_results, query)
        
        # Sort by combined score
        sorted_results = sorted(
            combined_results.values(),
            key=lambda x: x['combined_score'],
            reverse=True
        )[:15]  # Return top 15 for better fuzzy coverage
        
        return sorted_results
    
    def format_results(self, results):
        """Format results for display"""
        if not results:
            return "\nNo products found."
        
        # Check if any result has only neo4j_score and no qdrant_score (product code query)
        is_exact_search = any(result.get('qdrant_score', 0) == 0 and result.get('neo4j_score', 0) > 0 for result in results)
        search_type = "Exact Product Code Search" if is_exact_search else "Enhanced Fuzzy Search"
        output = [f"\nTop {len(results)} matching products ({search_type})"]
        
        for i, result in enumerate(results, 1):
            output.append(f"\n{i:2}. Product ID: {result['id']}")
            output.append(f"    Name: {result['name']}")
            output.append(f"    Combined Score: {result['combined_score']:.4f}")
            
            # Show detailed scoring
            details = f"    (Neo4j: {result['neo4j_score']:.3f}, Qdrant: {result['qdrant_score']:.3f}"
            if result.get('fuzzy_bonus', 0) > 0:
                details += f", Fuzzy: {result['fuzzy_bonus']:.3f}"
            details += ")"
            output.append(details)
        
        return "\n".join(output)
    
    def run_interactive_search(self):
        """Interactive search with enhanced fuzzy capabilities"""
        neo4j_weight = 0.6  # Higher weight for structured data
        qdrant_weight = 0.4
        
        while True:
            try:
                query = input("Enter search query: ").strip()
                
                if query.lower() == 'exit':
                    break
                elif query.lower() == 'weights':
                    try:
                        neo4j_weight = float(input(f"Neo4j weight (current: {neo4j_weight}): ") or neo4j_weight)
                        qdrant_weight = float(input(f"Qdrant weight (current: {qdrant_weight}): ") or qdrant_weight)
                        print(f"Weights updated: Neo4j={neo4j_weight}, Qdrant={qdrant_weight}")
                    except ValueError:
                        print("Invalid weight values. Keeping current weights.")
                    continue
                
                results = self.enhanced_hybrid_search(query, neo4j_weight, qdrant_weight)
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
    
    search_system = EnhancedHybridSearch(
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