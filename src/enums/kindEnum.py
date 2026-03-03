from enum import Enum

class KindMajorEnum(Enum):
    ORGANISATION = "Organisation" 
    PERSON = "Person"
    CATEGORY = "Category"
    DATASET = "Dataset"

class KindMinorEnum(Enum):
    DEPARTMENT = "department"
    STATE_MINISTER = "stateMinister"
    CABINET_MINISTER = "cabinetMinister"
    TABULAR = "tabular"
    CITIZEN = "citizen"
    PARENT_CATEGORY = "parentCategory"
    CHILD_CATEGORY = "childCategory"