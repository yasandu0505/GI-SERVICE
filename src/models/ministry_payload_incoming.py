from pydantic import BaseModel

class REQ_ONE(BaseModel):
    year : int
    govId : str
    presidentId : str
    dataSet : str