import os
import httpx
from typing import List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from .. import schemas, crud, database

from ..core.security import get_api_key, hash_patient_id
from ..core.config import STATUS_SIGNED, STATUS_STARTED

router = APIRouter(
    prefix="/measurements",
    tags=["measurements"],
    dependencies=[Depends(get_api_key)]
)

async def get_pacs_counts(study_id: str, iot_token: str):
    proxy_url = os.getenv('PACS_PROXY_URL')
    headers = {"Authorization": f"Bearer {iot_token}"}
    
    async with httpx.AsyncClient() as client:
        # Get series
        series_res = await client.get(f"{proxy_url}/studies/{study_id}/series", headers=headers)
        if series_res.status_code != 200:
            return 0, []
        
        series_list = series_res.json()
        series_len = len(series_list)
        instance_len = []
        
        for idx, series in enumerate(series_list, 1):
            series_uid = series.get("0020000E", {}).get("Value", [""])[0]
            # Get instance count for this series
            instances_res = await client.get(
                f"{proxy_url}/studies/{study_id}/series/{series_uid}/instances", 
                headers=headers
            )
            count = len(instances_res.json()) if instances_res.status_code == 200 else 0
            instance_len.append(schemas.SeriesInstanceCount(series_index=idx, instance_count=count))
            
        return series_len, instance_len

# --- Endpoint 1: Read All ---
@router.get("/", response_model=List[schemas.StudySummary])
async def list_measurements(
        skip: int = 0,
        limit: int = Query(default=25, le=1000),
        db: Session = Depends(database.get_db)
):
    referrals = crud.get_all_referrals(db, skip=skip, limit=limit, min_status=STATUS_STARTED)
    
    ris_url = os.getenv('RIS_API_URL')
    anonymizer_key = os.getenv('ANONYMIZER_API_KEY')

    results = []
    # Using a positional index for the loop to match study_idx
    for i, ref in enumerate(referrals, start=skip + 1):
        if len(ref.study_descriptions) > 0:
            # We need an IOT token to query counts from PACS
            async with httpx.AsyncClient() as client:
                # We can use the existing internal-token endpoint or the study-by-index one
                # Let's use study-by-index to be consistent with the anonymize endpoint
                ris_res = await client.get(
                    f"{ris_url}/referrals/study-by-index/{i}",
                    headers={"X-Anonymizer-Key": anonymizer_key}
                )
                
                if ris_res.status_code == 200:
                    data = ris_res.json()
                    iot_token = data['token']
                    series_len, instance_len = await get_pacs_counts(ref.study_id, iot_token)
                    
                    results.append(schemas.StudySummary(
                        index=i,
                        study_id=ref.study_id,
                        patient_id=hash_patient_id(ref.patient_id),
                        series_len=series_len,
                        instance_len=instance_len
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