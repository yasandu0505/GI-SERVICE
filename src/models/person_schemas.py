from pydantic import BaseModel
from datetime import date

class PersonSource(BaseModel):
    """
    Person source schema
    """
    name: str
    political_party: str
    date_of_birth: date
    religion: str
    profession: str
    email: str
    phone_number: str
    education_qualifications: str
    professional_qualifications: str
    image_url: str

class PersonResponse(PersonSource):
    """
    Person response schema inherited from the PersonSource
    """
    age: int
