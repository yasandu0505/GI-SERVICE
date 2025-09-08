from pydantic import BaseModel

class ATTRIBUTE_PAYLOAD(BaseModel):
    attribute_name : str
    chart_type : str