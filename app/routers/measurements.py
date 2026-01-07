from typing import List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from .. import schemas, crud, database

router = APIRouter(
    prefix="/measurements",
    tags=["measurements"]
)


# --- Endpoint 1: Read All (Summary List) ---
@router.get("/", response_model=List[schemas.StudySummary])
def list_measurements(
        skip: int = 0,
        limit: int = 100,
        db: Session = Depends(database.get_db)
):
    """
    Returns a lightweight list of all studies.
    Use the 'study_id' from this list to query specific details.
    """
    referrals = crud.get_all_referrals(db, skip=skip, limit=limit)

    results = []
    for ref in referrals:
        # Check if any descriptions exist
        has_data = len(ref.study_descriptions) > 0

        results.append(schemas.StudySummary(
            referral_id=ref.id,
            study_id=ref.study_id,
            patient_id=ref.patient_id,
            patient_name=f"{ref.patient_firstname} {ref.patient_lastname}",
            has_measurements=has_data
        ))
    return results


# --- Endpoint 2: Select by Study ID (Detail View) ---
@router.get("/{study_id}", response_model=List[schemas.SimpleStudyResponse])
def get_measurement_details(study_id: str, db: Session = Depends(database.get_db)):
    """
    Returns only patient_id and measurements for the given study_id.
    """
    ref = crud.get_referral_by_study_id(db, study_id)

    if not ref:
        raise HTTPException(status_code=404, detail="Study ID not found")

    results = []

    # If the referral exists but has no descriptions/measurements yet
    if not ref.study_descriptions:
        return [schemas.SimpleStudyResponse(
            patient_id=ref.patient_id,
            measurements=[]
        )]

    # Collect measurements from all descriptions associated with this study_id
    for desc in ref.study_descriptions:
        results.append(schemas.SimpleStudyResponse(
            patient_id=ref.patient_id,
            measurements=desc.measurements or []
        ))

    return results
