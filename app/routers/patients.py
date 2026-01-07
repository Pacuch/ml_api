from typing import List
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from .. import schemas, crud, database

router = APIRouter(
    prefix="/patient",
    tags=["patients"]
)


@router.get("/{patient_id}", response_model=List[schemas.PatientDataResponse])
def read_patient_data(patient_id: str, db: Session = Depends(database.get_db)):
    referrals = crud.get_referrals_by_patient_id(db, patient_id)

    if not referrals:
        raise HTTPException(status_code=404, detail="Patient not found")

    # Transform SQLAlchemy objects to Pydantic schema structure
    results = []
    for ref in referrals:
        descriptions = [
            schemas.MeasurementResponse(
                description_id=desc.id,
                description_text=desc.description,
                measurements=desc.measurements
            ) for desc in ref.study_descriptions
        ]

        results.append(schemas.PatientDataResponse(
            referral_id=ref.id,
            patient_id=ref.patient_id,
            first_name=ref.patient_firstname,
            last_name=ref.patient_lastname,
            study_data=descriptions
        ))

    return results