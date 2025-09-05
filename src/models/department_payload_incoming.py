from pydantic import BaseModel

class REQ_TWO(BaseModel):
    year : int
    gov : str
    president : str
    minister : str
    dataSet : str
    