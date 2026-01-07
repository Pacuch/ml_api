from typing import List, Optional, Any
from pydantic import BaseModel

# --- Shared Base ---
class MeasurementBase(BaseModel):
    description_id: int
    description_text: Optional[str]

    class Config:
        orm_mode = True

# --- 1. Summary Schema (Lightweight list) ---
class StudySummary(BaseModel):
    referral_id: int
    study_id: str             # The ID you want to select by
    patient_id: str
    has_measurements: bool    # Helper to see if data exists

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