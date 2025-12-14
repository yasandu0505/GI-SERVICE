import binascii
from datetime import datetime
import json
from google.protobuf.wrappers_pb2 import StringValue
from aiohttp import ClientSession, ClientTimeout
from typing import AsyncGenerator

async def get_http_session() -> AsyncGenerator[ClientSession, None]:
    timeout = ClientTimeout(total=90, connect=30, sock_connect=30, sock_read=90)
    async with ClientSession(timeout=timeout) as session:
        yield session

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