import os
import json
from datetime import datetime
import requests
import random
import string
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

                # Collect relation parts from parent folder back to base_path
                relation_parts = [parent_folder_name]  # folder before data.json
                current_dir = os.path.dirname(root)   # go one level up

                relatedEntityName = None

                while current_dir and current_dir != os.path.dirname(base_path):
                    folder_name = os.path.basename(current_dir)
                    relation_parts.append(folder_name)

                    # Pick the first non-(AS_CATEGORY) folder as relatedEntityName
                    if relatedEntityName is None and not folder_name.endswith("(AS_CATEGORY)"):
                        relatedEntityName = folder_name

                    current_dir = os.path.dirname(current_dir)

                # Reverse so relation starts from base_path down to parent folder
                relation_parts = list(reversed(relation_parts))
                relation = " - ".join(relation_parts)

                result.append({
                    "attributeName": parent_folder_name,
                    "relatedEntityName": relatedEntityName,
                    "relation": relation,
                    "attributeData": data_content
                })

        return result
    
    def pre_process_traverse_result(self, result):    
        for item in result:
            category_hop_count = 0
            relation_set = item["relation"].split(" - ")
            year = relation_set[0]
            date = datetime(int(year), 12, 31) 
            iso_date_str = date.strftime("%Y-%m-%dT%H:%M:%SZ")
            item["attributeReleaseDate"] = str(iso_date_str)
            for related_item in relation_set:
                if related_item.endswith("(government)"):
                    item["government"] = str(related_item.split('(')[0])
                elif related_item.endswith("(citizen)"):
                    item["president"] = str(related_item.split('(')[0])
                elif related_item.endswith("(minister)"):
                    item["minister"] = str(related_item.split('(')[0])
                elif related_item.endswith("(department)"):
                    item["department"] = str(related_item.split('(')[0])
                elif related_item.endswith("(AS_CATEGORY)"):
                    if "categoryData" not in item:
                        item["categoryData"] = {}
                    if category_hop_count == 0:
                        item["categoryData"]["parentCategory"] = str(related_item.split('(')[0])
                    elif category_hop_count >= 1:
                        item["categoryData"]["childCategory_" + str(category_hop_count)] = str(related_item.split('(')[0])
                        
                    category_hop_count += 1
                    
        return result
    
    
    def validate_node(self, entity_name, minorKind, majorKind) -> tuple[bool, str]:
        url = "http://0.0.0.0:8081/v1/entities/search"
        
        payload = {
            "id": "",
            "kind": {
                "major": majorKind,
                "minor": minorKind
            },
            "name": entity_name,
            "created": "",
            "terminated": ""
            }
        
        headers = {
                    "Content-Type": "application/json",
                    # "Authorization": f"Bearer {token}"  
                }
                
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()  
            output = response.json()
            if output:
                output = output["body"][0]["id"]
                return True , str(output)
        except Exception as e:
            print(f"error : " +  str(e))
            return False , "not found"
        
    def entity_validator(self, result):
        
        for item in result:
            for data in item:
                if data == "government":
                    minorKind = "government"
                    majorKind = "Organisation"
                    is_valid, entity_id = self.validate_node(item[data], minorKind, majorKind)
                    if is_valid:
                        item[data] = entity_id
                    else:
                        item[data] = entity_id
                elif data == "president":
                    minorKind = "citizen"
                    majorKind = "Person"
                    is_valid, entity_id  = self.validate_node(item[data], minorKind, majorKind)
                    if is_valid:
                        item[data] = entity_id
                    else:
                        item[data] = entity_id
                elif data == "minister":
                    minorKind = "minister"
                    majorKind = "Organisation"
                    is_valid, entity_id = self.validate_node(item[data], minorKind, majorKind)
                    if is_valid:
                        item[data] = entity_id
                    else:
                        item[data] = entity_id
                elif data == "department":
                    minorKind = "department"
                    majorKind = "Organisation"
                    is_valid, entity_id = self.validate_node(item[data], minorKind, majorKind)
                    if is_valid:
                        item[data] = entity_id
                    else:
                        item[data] = entity_id
                
        return result
    
    def create_nodes(self, node_id, node_name, node_key, date):
        
        url = "http://0.0.0.0:8080/entities/"
           
        payload = {
            "id": node_id,
            "kind": {
                "major": "Category",
                "minor": node_key
                },
            "created": date,
            "terminated": "",
            "name": {
                "startTime": date,
                "endTime": "",
                "value": node_name
            },
            "metadata": [],
            "attributes": [],
            "relationships": []
        }
        headers = {
                    "Content-Type": "application/json",
                    # "Authorization": f"Bearer {token}"  
                }
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()  
            output = response.json()
            return output

        except Exception as e:
            print(f"error : " +  str(e))
            return None
        
        
    def create_relationship(self, parent_id, child_id, date):
        url = f"http://0.0.0.0:8080/entities/{parent_id}"
        
        payload = {
                "id": parent_id,
                "kind": {},
                "created": "",
                "terminated": "",
                "name": {
                },
                "metadata": [],
                "attributes": [],
                "relationships": [
                 {
                    "key": "AS_CATEGORY",
                    "value": {
                        "relatedEntityId": child_id,
                        "startTime": date,
                        "endTime": "",
                        "id": f"{parent_id}-to-{child_id}",
                        "name": "AS_CATEGORY"
                    }
                }
            ]
        }
        headers = {
                    "Content-Type": "application/json",
                    # "Authorization": f"Bearer {token}"  
                }
        
        
        try:
            response = requests.put(url, json=payload, headers=headers)
            response.raise_for_status()  
            output = response.json()
            return output
        
        except Exception as e:
            print(f"error : " +  str(e))
            return None
            
    def generate_random_id(self,length=5):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    
    def create_relationship_with_main_parent(self, parent_of_parent_category, parent_id, date):
        url = f"http://0.0.0.0:8080/entities/{parent_of_parent_category}"
        
        payload = {
                "id": parent_of_parent_category,
                "kind": {},
                "created": "",
                "terminated": "",
                "name": {
                },
                "metadata": [],
                "attributes": [],
                "relationships": [
                 {
                    "key": "AS_CATEGORY",
                    "value": {
                        "relatedEntityId": parent_id,
                        "startTime": date,
                        "endTime": "",
                        "id": f"{parent_of_parent_category}-to-{parent_id}",
                        "name": "AS_CATEGORY"
                    }
                }
            ]
        }
        headers = {
                    "Content-Type": "application/json",
                    # "Authorization": f"Bearer {token}"  
                }
        
        
        try:
            response = requests.put(url, json=payload, headers=headers)
            response.raise_for_status()  
            output = response.json()
            return output
        
        except Exception as e:
            print(f"error : " +  str(e))
            return None
       
    
    def create_parent_categories_and_children_categories(self, result):
        count = 0
        node_ids = {}  

        for item in result:
            date = item["attributeReleaseDate"]
            if 'categoryData' in item:
                category_data = item['categoryData']

                # --- Create parent node ---
                parent_name = category_data['parentCategory']
                
                if 'minister' and 'department' in item:
                    parent_of_parent_category = item["department"]
                elif 'minister' in item:
                    parent_of_parent_category = item["minister"]
                
                attribute_name = item['attributeName']
                attribute_data = item['attributeData']
                
                if parent_name not in node_ids:
                    node_id = self.generate_random_id() + str(count)
                    print(f"Creating parent node '{parent_name}'...")
                    res = self.create_nodes(node_id, parent_name, 'parentCategory', date)
                    count += 1
                    node_id = res['id']
                    node_ids[parent_name] = node_id
                    print(f"Created parent node '{parent_name}' with id: {node_id}")
                else:
                    print(f"Parent node '{parent_name}' already exists with id: {node_ids[parent_name]}")
            
                parent_id = node_ids[parent_name]
                
                print(f"{parent_of_parent_category} - {parent_id}")
                
                self.create_relationship_with_main_parent(parent_of_parent_category, parent_id, date)

                # --- Create child nodes ---
                for key, child_name in category_data.items():
                    if key.startswith('childCategory'):
                        child_key = (parent_name, child_name)  # unique per parent
                        if child_key not in node_ids:
                            node_id = self.generate_random_id() + str(count)
                            print(f"Creating child node '{child_name}' for parent '{parent_name}'...")
                            res = self.create_nodes(node_id, child_name, key, date)
                            count += 1
                            node_id = res['id']
                            node_ids[child_key] = node_id
                            print(f"Created child node '{child_name}' with id: {node_id}")
                        else:
                            print(f"Child node '{child_name}' for parent '{parent_name}' already exists with id: {node_ids[child_key]}")

                        child_id = node_ids[child_key]

                        # --- Create relationship ---
                        res = self.create_relationship(parent_id, child_id, date)
                        first_rel = res['relationships'][0]
                        rel_key = first_rel['key']
                        print(f"Create relationship: {parent_name}({parent_id}) -> {child_name}({child_id}) with relationship id: {rel_key}")
                        self.create_attribute_to_entity(date, child_id, attribute_name, attribute_data)

                print("=" * 50) 
            else:
                if 'minister' and 'department' in item:
                    parent_of_attribute = item["department"]
                    print("attribute directly connected to a department") 
                elif 'minister' in item:
                    parent_of_attribute = item["minister"]
                    print("attribute directly connected to a ministry") 
                    
                print(f"there is a possible relationship from {parent_of_attribute} - {item['attributeName']}")
                attribute_name = item['attributeName']
                attribute_data = item['attributeData']
                
                self.create_attribute_to_entity(date, parent_of_attribute, attribute_name, attribute_data )
                print("attribute created.....=-========================================")
                
                
        return

    def create_attribute_to_entity(self, date, entity_id, attribute_name, values): 
        url = f"http://0.0.0.0:8080/entities/{entity_id}"
        payload = {
            "id": entity_id,
            "attributes": [
                {
                    "key": attribute_name,
                    "value": {
                        "values": [
                            {
                                "startTime": date,
                                "endTime": "",
                                "value": values
                            }
                        ]
                    }
                }
            ]
        }
        
        headers = {
                    "Content-Type": "application/json",
                    # "Authorization": f"Bearer {token}"  
                }
        
        try:
            response = requests.put(url, json=payload, headers=headers)
            response.raise_for_status()  
            output = response.json()
            return output
        except Exception as e:
            print(f"error : " +  str(e))
            return  