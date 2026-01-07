from typing import List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from .. import schemas, crud, database

from ..core.security import get_api_key, hash_patient_id
from ..core.config import STATUS_SIGNED

router = APIRouter(
    prefix="/measurements",
    tags=["measurements"],
    dependencies=[Depends(get_api_key)]  # Apply security globally here
)


# --- Endpoint 1: Read All ---
@router.get("/", response_model=List[schemas.StudySummary])
def list_measurements(
        skip: int = 0,
        limit: int = Query(default=25, le=1000),
        db: Session = Depends(database.get_db)
):
    # Note: Added STATUS_SIGNED here based on your previous request
    referrals = crud.get_all_referrals(db, skip=skip, limit=limit, status=STATUS_SIGNED)

    results = []
    for ref in referrals:
        if len(ref.study_descriptions) > 0:
            results.append(schemas.StudySummary(
                referral_id=ref.id,
                study_id=ref.study_id,
                patient_id=hash_patient_id(ref.patient_id)  # Clean function call
            ))
    return results


# --- Endpoint 2: Detail View ---
@router.get("/{study_id}", response_model=List[schemas.SimpleStudyResponse])
def get_measurement_details(study_id: str, db: Session = Depends(database.get_db)):
    ref = crud.get_referral_by_study_id(db, study_id)

    if not ref:
        raise HTTPException(status_code=404, detail="Study ID not found")

    hashed_id = hash_patient_id(ref.patient_id)

    if not ref.study_descriptions:
        return [schemas.SimpleStudyResponse(
            patient_id=hashed_id,
            measurements=[]
        )]

    return [
        schemas.SimpleStudyResponse(
            patient_id=hashed_id,
            measurements=desc.measurements or []
        )
        for desc in ref.study_descriptions
    ]