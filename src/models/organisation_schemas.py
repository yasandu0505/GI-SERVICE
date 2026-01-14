from pydantic import BaseModel

class Label(BaseModel):
    """Label refers to an entity with id and name"""
    id: str = ""
    name: str = ""

class Kind(BaseModel):
    """Kind refers to the type of entity in the OpenGIN Specification"""
    major: str = ""
    minor: str = ""
    
class Entity(BaseModel):
    """Entity refers to the object in the OpenGIN Specification"""
    id: str = ""
    name: str = ""
    kind: Kind = Kind()
    created: str = ""
    terminated: str = ""

class Relation(BaseModel):
    """Relation refers to the relation between two entities in the OpenGIN Specification"""
    name: str = ""
    activeAt: str = ""
    relatedEntityId: str = ""
    startTime: str = ""
    endTime: str = ""
    id: str = ""
    direction: str = ""

class Category(BaseModel):
    """Category refers to the parent/child category in the OpenGIN Specification"""
    id: str = ""
    name: str = ""
    kind: Kind = Kind()

class Dataset(BaseModel):
    """Dataset refers to the dataset in the OpenGIN Specification"""
    id: str = ""
    label: Label = Label()
    parentId: str = ""
    kind: Kind = Kind()

class Date(BaseModel):
    date: str

