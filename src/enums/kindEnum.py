from enum import Enum

class KindMajorEnum(Enum):
    ORGANISATION = "Organisation" 
    PERSON = "Person"
    CATEGORY = "Category"
    DATASET = "Dataset"
    DOCUMENT = "Document"

class KindMinorEnum(Enum):
    DEPARTMENT = "department"
    STATE_MINISTER = "stateMinister"
    CABINET_MINISTER = "cabinetMinister"
    TABULAR = "tabular"
    CITIZEN = "citizen"
    PARENT_CATEGORY = "parentCategory"
    CHILD_CATEGORY = "childCategory"
    EXTGZT_ORGANISATION = "extgztorg"
    EXTGZT_PERSON = "extgztperson"