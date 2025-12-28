from pydantic import BaseModel

class Kind(BaseModel):
    major: str = ""
    minor: str = ""
    
class Entity(BaseModel):
    id: str = ""
    name: str = ""
    kind: Kind = Kind()
    created: str = ""
    terminated: str = ""

class Date(BaseModel):
    date: str

