import binascii
import json
import re
from datetime import datetime
from google.protobuf.wrappers_pb2 import StringValue
from google.protobuf import struct_pb2
from google.protobuf.json_format import MessageToDict

class Util:
    # helper: normalize timestamp
    @staticmethod
    def normalize_timestamp(time_stamp: str | None) -> str | None:
            """
            Ensure timestamp is in ISO format expected by downstream services.
            Accepts dates like '2022-05-05' and converts to '2022-05-05T00:00:00Z'.
            """
            if not time_stamp:
                return time_stamp

            ts = time_stamp.strip()

            try:
                # Convert to standard ISO with Z suffix
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                # Fallback for date without time component
                if "T" not in ts:
                    return f"{ts}T00:00:00Z"
                if ts.endswith("Z"):
                    return ts
                return f"{ts}Z"

    # helper: decode protobuf attribute name 
    @staticmethod      
    def decode_protobuf_attribute_name(name : str) -> str: 
            try:
                data = json.loads(name)
                hex_value = data.get("value")
                if not hex_value:
                    return "Unknown"

                decoded_bytes = binascii.unhexlify(hex_value)
                
                sv = StringValue()
                try:
                    sv.ParseFromString(decoded_bytes)
                    if(sv.value.strip() == ""):
                        return decoded_bytes.decode("utf-8", errors="ignore").strip()
                    return sv.value.strip()
                except Exception:
                    decoded_str = decoded_bytes.decode("utf-8", errors="ignore")
                    cleaned = ''.join(ch for ch in decoded_str if ch.isprintable())
                    return cleaned.strip()
            except Exception as e:
                print(f"[DEBUG decode] outer exception: {e}")
                return "Unknown"

    # helper: term helper
    @staticmethod
    def term(startTime, endTime) -> str:
        """
        Generate a term string based on start and end dates.

        startDate format: YYYY-MM-DDT00:00:00Z
        endDate format: YYYY-MM-DDT00:00:00Z

        return format:
            YYYY MM - YYYY MM
            YYYY MM - Present

            ex: 2020 Jan - 2022 May
                2020 Jan - Present
        """

        if not startTime:
            return "Unknown"
        
        start_date = startTime.split("T")[0]

        start_date_object = datetime.strptime(start_date, "%Y-%m-%d")
        start_year = start_date_object.year
        start_month_abbr = start_date_object.strftime("%b")

        if not endTime or endTime == "":
            end_year = "Present"
            term = f"{start_year} {start_month_abbr} - {end_year}"
        else:
            end_date = endTime.split("T")[0]
            end_date_object = datetime.strptime(end_date, "%Y-%m-%d")
            end_year = end_date_object.year
            end_month_abbr = end_date_object.strftime("%b")
            term = f"{start_year} {start_month_abbr} - {end_year} {end_month_abbr}"

        return term

    # helper: to convert string to title case
    @staticmethod
    def to_title_case(text: str) -> str:
        """
        Converts any string to proper title case with each word capitalized.
        Removes special characters except spaces.
        
        Args:
            text (str): The input string to convert
            
        Returns:
            str: The formatted string in title case
            
        Examples:
            to_title_case("my nAme is?") -> "My Name Is"
            to_title_case("hello_world-test") -> "Hello World Test"
            to_title_case("THIS is A TEST!!!") -> "This Is A Test"
        """
        if not text:
            return ""
        
        text = text.replace('_', ' ').replace('-', ' ')
        text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
        words = [word.capitalize() for word in text.split() if word]
        return ' '.join(words)

    @staticmethod
    def get_name_without_year(name: str) -> str:
        """
        Removes the year from the name if it exists at the end.
        
        Args:
            name (str): The name to remove the year from.
            
        Returns:
            str: The name without the year at the end split by '-'.
        """
        if not name:
            return ""
        
        return re.sub(r"-\d{4}$", "", name)
    
    @staticmethod
    def decode_response(response):
        """
        Decode the protobuf response and extract the data
        """
        response_value = response["value"]
            
        # Extract the hex string
        hex_value = json.loads(response_value)["value"]
        
        # Convert hex string to bytes
        proto_bytes = bytes.fromhex(hex_value)
        
        # Parse into a Protobuf Struct
        struct_obj = struct_pb2.Struct()
        struct_obj.ParseFromString(proto_bytes)
        
        # Convert to a Python dictionary
        return MessageToDict(struct_obj)
        
    @staticmethod
    def transform_data_for_chart(attribute_data_out):
        """
        Transform decoded attribute data into a generic format with automatic type detection.
        
        The function automatically identifies the data type by inspecting the structure:
        - Tabular: Has "columns" and "rows" keys
        - Graph: Has "nodes" and "edges" keys
        - Document: Has "content" key or other document-like structure
        - Unknown: Doesn't match any known pattern
        
        Args:
            attribute_data_out: Dictionary containing the decoded data
            
        Returns:
            dict: Formatted data with structure:
                {
                    "type": "<auto_detected_type>",
                    "data": {
                        // Type-specific data structure
                    }
                }
        """
        if attribute_data_out.get("error"):
            return {
                "type": "unknown",
                "error": attribute_data_out["error"]
            }
        
        try:
            decoded_data = Util.decode_response(attribute_data_out["data"])
        
            if not decoded_data:
                return {
                    "type": "unknown",
                    "error": "Could not decode response"
                }
        
            data_dictionary = json.loads(decoded_data["data"])
            
            # Auto-detect data type based on structure
            detected_type = Util.detect_data_type(data_dictionary)
            
            # Handle different data types
            if detected_type == "tabular":
                # Tabular data with columns and rows
                columns = data_dictionary.get("columns", [])
                rows = data_dictionary.get("rows", [])
                
                return {
                    "type": "tabular",
                    "data": {
                        "columns": columns,
                        "rows": rows
                    }
                }
            
            else:
                # Unknown type - return raw data
                return {
                    "type": "unknown",
                    "data": data_dictionary
                }
                
        except Exception as e:
            return {
                "type": "unknown",
                "error": f"Failed to transform data: {str(e)}"
            }
    
    @staticmethod
    def detect_data_type(data_dict: dict) -> str:
        """
        Detect the data type based on the structure of the data dictionary.
        
        Args:
            data_dict: The data dictionary to inspect
            
        Returns:
            str: The detected data type ("tabular" or "unknown")
        """
        if not isinstance(data_dict, dict):
            return "unknown"
        
        # Check for tabular data (has both columns and rows)
        if "columns" in data_dict and "rows" in data_dict:
            return "tabular"
        
        # Default to unknown
        return "unknown"

    @staticmethod
    def extract_year(date_string: str) -> int:
        """
        Extract year from a date string.

        Handles formats:
            - YYYY-MM-DD
            - YYYY-MM-DDTHH:MM:SSZ

        Args:
            date_string: Date string to parse

        Returns:
            Year as integer, or 9999 if parsing fails
        """
        if not date_string:
            return 9999

        try:
            return int(date_string.split("-")[0])
        except (ValueError, IndexError):
            return 9999

    @staticmethod
    def calculate_match_score(query: str, text: str) -> float:
        """
        Calculate relevance score for search match.

        Scoring:
            - Exact match: 1.0
            - Starts with query: 0.8
            - Contains query: 0.6
            - No match: 0.0

        Args:
            query: Search query string
            text: Text to match against

        Returns:
            Float score between 0.0 and 1.0
        """
        if not text:
            return 0.0

        query_lower = query.lower().strip()
        text_lower = text.lower().strip()

        if text_lower == query_lower:
            return 1.0
        elif text_lower.startswith(query_lower):
            return 0.8
        elif query_lower in text_lower:
            return 0.6
        else:
            return 0.0
    
