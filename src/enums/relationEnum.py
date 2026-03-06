from enum import Enum

# relation names 
class RelationNameEnum(Enum):
    AS_MINISTER = "AS_MINISTER"
    AS_APPOINTED = "AS_APPOINTED"
    AS_CATEGORY = "AS_CATEGORY"
    IS_ATTRIBUTE = "IS_ATTRIBUTE"
    AS_PRIME_MINISTER = "AS_PRIME_MINISTER"
    AS_PRESIDENT = "AS_PRESIDENT"
    AS_DEPARTMENT = "AS_DEPARTMENT"

# relation directions 
class RelationDirectionEnum(Enum):
    OUTGOING = "OUTGOING"
    INCOMING = "INCOMING"