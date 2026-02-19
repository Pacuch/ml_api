from typing import List, Optional, Any
from pydantic import BaseModel

# --- Shared Base ---
class MeasurementBase(BaseModel):
    description_id: int
    description_text: Optional[str]

    class Config:
        orm_mode = True

# --- 1. Summary Schema (Lightweight list) ---
class SeriesInstanceCount(BaseModel):
    series_index: int
    instance_count: int

class StudySummary(BaseModel):
    index: int                # Database index (referral_id)
    study_id: str             # StudyInstanceUID
    patient_id: str
    series_len: int           # Number of series
    instance_len: List[SeriesInstanceCount] # Instances per series

    class Config:
        orm_mode = True

# --- 2. Detail Schema (Full Data) ---
class StudyDetail(StudySummary):
    measurements: List[Any]   # The heavy JSON data comes here

class SimpleStudyResponse(BaseModel):
    patient_id: str
    measurements: Any  # This will hold the JSON list from the DB

    class Config:
        orm_mode = True