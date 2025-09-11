import os
import json

class WriteAttributes:
    
    def traverse_folder(self, base_path):
        result = []

        for root, dirs, files in os.walk(base_path):
            if 'data.json' in files and 'metadata.json' in files:
                data_path = os.path.join(root, 'data.json')
                parent_folder_name = os.path.basename(root)

                try:
                    with open(data_path, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                        if not content:
                            print(f"Skipping empty data.json in {root}")
                            continue
                        data_content = json.loads(content)
                except json.JSONDecodeError as e:
                    print(f"Skipping invalid JSON in {root}: {e}")
                    continue
                except Exception as e:
                    print(f"Error reading {data_path}: {e}")
                    continue

                # Build relation path back to relatedEntityName
                relation_parts = [parent_folder_name]  # start with folder before data.json
                current_dir = os.path.dirname(root)   # go one level up

                relatedEntityName = None

                while current_dir and current_dir != base_path:
                    folder_name = os.path.basename(current_dir)
                    relation_parts.append(folder_name)

                    if not folder_name.endswith("(AS_CATEGORY)"):
                        relatedEntityName = folder_name
                        break

                    current_dir = os.path.dirname(current_dir)

                # Reverse relation parts to start from relatedEntityName
                if relatedEntityName:
                    relation_parts = list(reversed(relation_parts))
                    relation = " - ".join(relation_parts)
                else:
                    relation = None

                result.append({
                    "attributeName": parent_folder_name,
                    "relatedEntityName": relatedEntityName,
                    "relation": relation,
                    "attributeData": data_content
                })

        return result