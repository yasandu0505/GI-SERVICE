import binascii
import json
import re
from datetime import datetime
from google.protobuf.wrappers_pb2 import StringValue

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
        
        return name.split("-")[0]