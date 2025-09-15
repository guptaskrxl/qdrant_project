# import json
# import re

# json_file_path = "cleaned_relevant_attributes.json" 
# with open(json_file_path, 'r', encoding='utf-8') as f:
#     products = json.load(f)

# print(products[0]["description"])


########################################################
# import json

# records = []
# with open("products_backup-08_19_2025.json", "r") as infile:
#     for i, line in enumerate(infile):
#         records.append(json.loads(line))

# with open("products.json", "w") as outfile:
#     json.dump(records, outfile, indent=2)

########################################################

# import json
# import random

# in_path = "cleaned_relevant_attributes.json"
# out_path = "products_50.json"
# k = 50

# # Optional: reproducibility
# # random.seed(42)  # [for deterministic runs]

# # Pass 1: count lines (assumes JSON Lines format: one JSON object per line)
# with open(in_path, "r", encoding="utf-8") as f:
#     n = sum(1 for _ in f)

# if k > n:
#     raise ValueError(f"Requested k={k} > number of records n={n}")

# # Pick k unique line indices uniformly at random
# chosen = set(random.sample(range(n), k))  # sampling without replacement

# # Pass 2: collect selected records
# records = []
# with open(in_path, "r", encoding="utf-8") as f:
#     for i, line in enumerate(f):
#         if i in chosen:
#             records.append(json.loads(line))

# with open(out_path, "w", encoding="utf-8") as out:
#     json.dump(records, out, indent=2, ensure_ascii=False)

#############################################################

# import json
# import random

# in_path = "final_data_qdrant.json"
# out_path = "products_5.json"
# k = 5

# # Optional: reproducible sampling
# # random.seed(42)

# with open(in_path, "r", encoding="utf-8") as f:
#     data = json.load(f)  # data is a Python list of dicts

# # Guard if fewer than k items
# k = min(k, len(data))

# sampled = random.sample(data, k)  # uniform, without replacement

# with open(out_path, "w", encoding="utf-8") as out:
#     json.dump(sampled, out, indent=2, ensure_ascii=False)


#############################################################

# import json

# # Input and output file paths
# input_file = "products_backup-08_19_2025.json"       # your input NDJSON file
# output_file = "filtered_products_neo4j.json"  # new JSON file

# # Fields to extract
# fields_to_extract = [
#     "id", "name", "short_description", "filterAttributes", "miscAttributes", "configAttributes", "keyAttributes"
# ]

# filtered_products = []

# # Read NDJSON line by line
# with open(input_file, "r", encoding="utf-8") as f:
#     for line in f:
#         line = line.strip()
#         if not line:
#             continue  # skip empty lines

#         try:
#             obj = json.loads(line)  # each line is a JSON object
#             source = obj.get("_source", {})

#             # Extract only the required fields
#             filtered = {field: source.get(field, None) for field in fields_to_extract}
#             filtered_products.append(filtered)
#         except json.JSONDecodeError:
#             print(f"Skipping invalid line: {line[:50]}...")

# # Save into a new JSON file
# with open(output_file, "w", encoding="utf-8") as f:
#     json.dump(filtered_products, f, ensure_ascii=False, indent=2)

# print(f"✅ Extracted {len(filtered_products)} products to {output_file}")

##################################################################

# import json
# import re

# # Input and output file paths
# input_file = "filtered_products_neo4j.json"
# output_file = "final_data_neo4j.json"

# # Load JSON
# with open(input_file, "r", encoding="utf-8") as f:
#     products = json.load(f)

# # Clean all descriptions
# for product in products:
#     if "description" in product and isinstance(product["description"], str):
#         # Remove bullets at start of line
#         cleaned = re.sub(r'^[●•]\s*', '', product["description"], flags=re.MULTILINE)
#         # Replace line breaks with space
#         cleaned = cleaned.replace("\n", " ")
#         # Collapse extra spaces (optional)
#         cleaned = re.sub(r'\s+', ' ', cleaned).strip()
#         product["description"] = cleaned

# for product in products:
#     if "short_description" in product and isinstance(product["short_description"], str):
#         # Remove bullets at start of line
#         cleaned = re.sub(r'^[●•]\s*', '', product["short_description"], flags=re.MULTILINE)
#         # Replace line breaks with space
#         cleaned = cleaned.replace("\n", " ")
#         # Collapse extra spaces (optional)
#         cleaned = re.sub(r'\s+', ' ', cleaned).strip()
#         product["short_description"] = cleaned

# # Save cleaned JSON into a new file
# with open(output_file, "w", encoding="utf-8") as f:
#     json.dump(products, f, ensure_ascii=False, indent=2)


###################################################################

# import json

# # Load your JSON (replace 'data.json' with your actual file path)
# with open("final_data_qdrant.json", "r") as f:
#     data = json.load(f)

# def is_empty(value):
#     return not isinstance(value, str) or not value.strip()

# empty_both = sum(
#     1 for item in data
#     if is_empty(item.get("description")) and is_empty(item.get("short_description"))
# )

# print(f"Number of blocks with empty description AND empty short_description: {empty_both}")

#check for item in name
# import json

# with open("final_data_neo4j.json", "r") as f:
#     data = json.load(f)

# search_str = "CX-112"

# matches = [
#     item for item in data
#     if isinstance(item.get("name"), str) and search_str in item["name"]
# ]

# print(f"Number of matches: {len(matches)}")
# for m in matches:
#     print(m["id"], "->", m["name"])