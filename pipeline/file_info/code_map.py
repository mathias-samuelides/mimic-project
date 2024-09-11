from enum import StrEnum
from pipeline.file_info.path_prefix import MAPPING_PATH

MAP_PATH = MAPPING_PATH / "ICD9_to_ICD10_mapping.txt"
MAP_NDC_PATH = MAPPING_PATH / "ndc_product.txt"


# icd mapping
class IcdMap(StrEnum):
    DIAGNOISIS_TYPE = "diagnosis_type"
    DIAGNOISIS_CODE = "diagnosis_code"
    DIAGNOISIS_DESCRIPTION = "diagnosis_description"
    ICD9 = "icd9cm"
    ICD10 = "icd10cm"
    FLAGS = "flags"


class NdcMap(StrEnum):
    NON_PROPRIETARY_NAME = "NONPROPRIETARYNAME"
