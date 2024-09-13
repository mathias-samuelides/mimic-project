from enum import StrEnum
from pipeline.file_info.path_prefix import MAPPING_PATH

MAP_PATH = MAPPING_PATH / "ICD9_to_ICD10_mapping.txt"
MAP_NDC_PATH = MAPPING_PATH / "ndc_product.txt"


class IcdMapHeader(StrEnum):
    DIAGNOSIS_TYPE = "diagnosis_type"  # Type of diagnosis
    DIAGNOSIS_CODE = "diagnosis_code"  # The specific ICD diagnosis code
    DIAGNOSIS_DESCRIPTION = "diagnosis_description"  # A description of the diagnosis
    ICD9 = "icd9cm"  # The ICD-9 diagnosis code (old version)
    ICD10 = "icd10cm"  # The ICD-10 diagnosis code (current version)
    FLAGS = "flags"  # Additional flags or metadata for the diagnosis (e.g., severity indicators, special notes)


class NdcMapHeader(StrEnum):
    PRODUCT_NDC = "productndc"  # The NDC code for the product (identifies the drug)
    NON_PROPRIETARY_NAME = "nonproprietaryname"  # The generic name of the drug
    PHARM_CLASSES = "pharm_classes"  # Pharmacological classes of the drug (indicates drug class or category)
    NEW_NDC = "new_ndc"  # A formatted or normalized version of the NDC code (used for consistent merging)
