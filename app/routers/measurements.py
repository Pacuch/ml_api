import os
import httpx
import logging
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

logger = logging.getLogger("uvicorn")

async def get_pacs_counts(study_id: str, iot_token: str):
    proxy_url = os.getenv('PACS_PROXY_URL')
    headers = {"Authorization": f"Bearer {iot_token}"}
    logger.info(f"DEBUG: Starting get_pacs_counts for {study_id} using proxy {proxy_url}")
    
    async with httpx.AsyncClient() as client:
        try:
            # Get series
            series_res = await client.get(f"{proxy_url}/studies/{study_id}/series", headers=headers, timeout=10.0)
            logger.info(f"DEBUG: Series response for {study_id}: {series_res.status_code}")
            
            if series_res.status_code != 200:
                logger.error(f"PACS Series Error: {series_res.status_code} Body: {series_res.text[:100]}")
                return 0, []
            
            series_list = series_res.json()
            series_len = len(series_list)
            instance_len = []
            
            logger.info(f"DEBUG: Found {series_len} series for study {study_id}")
            
            for idx, series in enumerate(series_list, 1):
                series_uid = series.get("0020000E", {}).get("Value", [""])[0]
                logger.info(f"DEBUG: Fetching instances for series {idx}: {series_uid}")
                
                instances_res = await client.get(
                    f"{proxy_url}/studies/{study_id}/series/{series_uid}/instances", 
                    headers=headers,
                    timeout=10.0
                )
                
                if instances_res.status_code == 200:
                    instances = instances_res.json()
                    count = len(instances)
                    logger.info(f"DEBUG: Series {idx} returned {count} instances.")
                else:
                    logger.error(f"PACS Instance Error for series {idx}: {instances_res.status_code}")
                    count = 0
                
                instance_len.append(schemas.SeriesInstanceCount(series_index=idx, instance_count=count))
                
            return series_len, instance_len
        except Exception as e:
            logger.error(f"get_pacs_counts CRITICAL exception: {str(e)}")
            return 0, []

# --- Endpoint 1: Read All ---
@router.get("/", response_model=List[schemas.StudySummary])
async def list_measurements(
        skip: int = 0,
        limit: int = Query(default=25, le=1000),
        db: Session = Depends(database.get_db)
):
    # Temporarily RELAX filters to see what's happening
    referrals = crud.get_all_referrals(db, skip=skip, limit=limit)
    
    # ris_url = os.getenv('RIS_API_URL', "").rstrip('/')
    ris_url = "http://apiserver:8000/app/api"
    anonymizer_key = os.getenv('ANONYMIZER_API_KEY', "")

    logger.info(f"DEBUG: Starting measurements sync. RIS_URL={ris_url}")
    
    if not ris_url:
        raise HTTPException(status_code=500, detail="RIS_API_URL not configured")

    results = []
    logger.info(f"DEBUG: Processing {len(referrals)} referrals from DB")

    for i, ref in enumerate(referrals, start=skip + 1):
        has_desc = len(ref.study_descriptions) > 0
        if has_desc:
            async with httpx.AsyncClient() as client:
                # Construct URL carefully
                target_url = f"{ris_url}/referrals/study-by-index/{i}/"
                logger.info(f"DEBUG: Fetching token from: {target_url}")
                ris_res = await client.get(
                    target_url,
                    headers={"X-Anonymizer-Key": anonymizer_key},
                    timeout=5.0
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
                else:
                    logger.error(f"RIS Token Error: {ris_res.status_code} for index {i}")
                    # Still add it without PACS counts if token fails
                    results.append(schemas.StudySummary(
                        index=i,
                        study_id=ref.study_id,
                        patient_id=hash_patient_id(ref.patient_id),
                        series_len=0,
                        instance_len=[]
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
