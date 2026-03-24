import os
import io
import httpx
import pydicom
import zipfile
import json
import hashlib
import re
import logging
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, HTTPException, Header, Response, Request, Depends
from fastapi.responses import StreamingResponse
from pydicom.uid import generate_uid

from ..core.security import get_api_key

router = APIRouter(prefix="/anonym", tags=["anonymization"])
logger = logging.getLogger("uvicorn")

# --- Core Anonymization Engine (Ported from anonym.py) ---

class AnonymizerEngine:
    def __init__(self):
        self.rules = self._load_rules()
        self.pepper = os.getenv("PEPPER", "default_secret_pepper")
        self.uid_map = {}

    def _load_rules(self):
        # Path to your JSON profile
        config_path = os.path.join(os.path.dirname(__file__), "..", "..", "anonym", "config", "dicom_ps3_15_profile.json")
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Could not load anonymization config: {e}")
            return {}

    def _get_replacement_value(self, vr, action):
        if action == 'D': return ""
        static_defaults = {
            'DA': "", 'TM': "", 'DT': "",
            'PN': "ANONYMIZED", 'AS': "000Y", 'CS': "U", 'DS': "0",
            'IS': "0", 'LO': "ANONYMIZED", 'SH': "ANONYMIZED",
            'ST': "ANONYMIZED", 'LT': "ANONYMIZED", 'UT': "ANONYMIZED", 'AE': "ANONYMIZED",
        }
        return generate_uid() if vr == 'UI' else static_defaults.get(vr, "ANONYMIZED")

    def _generate_consistent_uid(self, original_uid):
        if original_uid not in self.uid_map:
            self.uid_map[original_uid] = generate_uid()
        return self.uid_map[original_uid]

    def _process_dataset_recursive(self, dataset):
        try:
            dataset.remove_private_tags()
        except:
            pass

        for elem in list(dataset):
            keyword = elem.keyword
            if not keyword: continue

            # --- SEQUENCE HANDLING ---
            if elem.VR == 'SQ':
                if keyword in self.rules and self.rules[keyword] == 'X':
                    delattr(dataset, keyword)
                else:
                    for item in elem.value: 
                        self._process_dataset_recursive(item)
                continue

            # --- STANDARD PROFILE RULES ---
            if keyword in self.rules:
                action = self.rules[keyword]

                if action == 'X':
                    delattr(dataset, keyword)
                elif action == 'U':
                    if elem.value:
                        if elem.VM > 1:
                            elem.value = [self._generate_consistent_uid(u) for u in elem.value]
                        else:
                            elem.value = self._generate_consistent_uid(elem.value)
                elif action in ['Z', 'D']:
                    elem.value = self._get_replacement_value(elem.VR, action)

    def anonymize_dataset(self, ds):
        # 1. Recursive processing of the dataset
        self._process_dataset_recursive(ds)

        # 2. Final mandatory metadata
        today = datetime.now().strftime('%Y%m%d')
        ds.StudyDate = ds.SeriesDate = ds.ContentDate = today
        
        ds.PatientIdentityRemoved = "YES"
        ds.DeidentificationMethod = "DICOM PS3.15 Basic Application Level Confidentiality Profile"
        
        return ds

# Global instance
engine = AnonymizerEngine()

# --- Helpers ---

def extract_study_id(path: str) -> Optional[str]:
    match = re.search(r'studies/([^/]+)', path)
    if match:
        return match.group(1)
    return None

async def get_internal_token(study_id: str) -> str:
    ris_url = os.getenv('RIS_API_URL', "http://apiserver:8000/app/api")
    anonymizer_key = os.getenv('ANONYMIZER_API_KEY', "")
    
    if not anonymizer_key:
        raise HTTPException(status_code=500, detail="ANONYMIZER_API_KEY not configured in ML API")
        
    async with httpx.AsyncClient() as client:
        ris_res = await client.get(
            f"{ris_url}/referrals/study-by-uid/{study_id}/",
            headers={"X-Anonymizer-Key": anonymizer_key}
        )
        if ris_res.status_code != 200:
            raise HTTPException(status_code=ris_res.status_code, detail=f"Failed to find study {study_id} in RIS")
            
        return ris_res.json()['token']

def unpack_multipart_dicom(content: bytes, content_type: str) -> Optional[bytes]:
    """
    Simplistic parser for multipart/related content to extract the DICOM part.
    """
    match = re.search(r'boundary=([^;]+)', content_type)
    if not match:
        return None
        
    boundary = match.group(1).strip('"')
    parts = content.split(f"--{boundary}".encode())
    
    for part in parts:
        if b"application/dicom" in part or b"DICM" in part:
            # Find start of body (after headers)
            header_end = part.find(b"\r\n\r\n")
            if header_end != -1:
                return part[header_end+4:].rstrip(b"\r\n--")
            
    return None

# --- Endpoints ---

@router.get("/{path:path}")
async def anonymize_proxy_path(
    path: str, 
    request: Request,
    api_key: Optional[str] = Depends(get_api_key)
):
    proxy_base_url = os.getenv('PACS_PROXY_URL')
    if not proxy_base_url:
        proxy_base_url = "http://pacs-proxy:8080"

    target_path = path
    if path.startswith("pacs/"):
        target_path = path[len("pacs/"):]
    
    target_url = f"{proxy_base_url.rstrip('/')}/{target_path.lstrip('/')}"
    
    # Forward headers (Authorization, etc.)
    headers = dict(request.headers)
    headers.pop("host", None)
    
    # Force requesting application/dicom to avoid multipart JPEGs if possible
    headers["Accept"] = 'application/dicom, multipart/related; type="application/dicom"'

    # PSK Auth logic
    if api_key and not headers.get("authorization"):
        study_id = extract_study_id(path)
        if study_id:
            try:
                iot_token = await get_internal_token(study_id)
                headers["Authorization"] = f"Bearer {iot_token}"
            except Exception as e:
                print(f"Internal auth failed for study {study_id}: {e}")

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                target_url, 
                headers=headers, 
                params=request.query_params, 
                follow_redirects=True, 
                timeout=30.0
            )
            
            if response.status_code != 200:
                return Response(
                    content=response.content, 
                    status_code=response.status_code, 
                    media_type=response.headers.get("content-type")
                )
            
            content_type = response.headers.get("content-type", "")
            raw_content = response.content
            
            # Handle Multipart
            if "multipart/related" in content_type:
                unpacked = unpack_multipart_dicom(raw_content, content_type)
                if unpacked:
                    raw_content = unpacked
                    content_type = "application/dicom"

            # Check if content is DICOM
            is_dicom = "application/dicom" in content_type or \
                       raw_content.startswith(b'\x00' * 128 + b'DICM') or \
                       (len(raw_content) > 132 and raw_content[128:132] == b'DICM')

            if is_dicom:
                try:
                    ds = pydicom.dcmread(io.BytesIO(raw_content), force=True)
                    ds = engine.anonymize_dataset(ds)
                    
                    out_buf = io.BytesIO()
                    ds.save_as(out_buf)
                    return Response(content=out_buf.getvalue(), media_type="application/dicom")
                except Exception as e:
                    print(f"Failed to anonymize DICOM: {e}")
                    return Response(content=raw_content, media_type="application/dicom")
            
            # For JPEGs or other types, we return as-is but force Content-Type if we unpacked it
            return Response(content=raw_content, media_type=content_type)
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Proxy error: {str(e)}")


@router.get("/study/{study_id}")
async def anonymize_study(
    study_id: str, 
    x_iot_token: Optional[str] = Header(None),
    api_key: Optional[str] = Depends(get_api_key)
):
    iot_token = x_iot_token
    if not iot_token and api_key:
        iot_token = await get_internal_token(study_id)
        
    if not iot_token:
        raise HTTPException(status_code=401, detail="Missing X-IOT-Token header or API Key")

    proxy_base_url = os.getenv('PACS_PROXY_URL', "http://pacs-proxy:8080")
    
    async def generate_zip():
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            headers = {
                "Authorization": f"Bearer {iot_token}",
                "Accept": "application/dicom"
            }
            
            async with httpx.AsyncClient() as client:
                instances_res = await client.get(f"{proxy_base_url}/studies/{study_id}/instances", headers=headers)
                if instances_res.status_code != 200: return
                
                instances = instances_res.json()
                for idx, inst in enumerate(instances):
                    series_uid = inst.get("0020000E", {}).get("Value", [""])[0]
                    instance_uid = inst.get("00080018", {}).get("Value", [""])[0]
                    
                    file_res = await client.get(
                        f"{proxy_base_url}/studies/{study_id}/series/{series_uid}/instances/{instance_uid}/frames/1",
                        headers=headers
                    )
                    
                    if file_res.status_code == 200:
                        content = file_res.content
                        c_type = file_res.headers.get("content-type", "")
                        if "multipart/related" in c_type:
                            unpacked = unpack_multipart_dicom(content, c_type)
                            if unpacked: content = unpacked
                        
                        try:
                            ds = pydicom.dcmread(io.BytesIO(content), force=True)
                            ds = engine.anonymize_dataset(ds)
                            out_buf = io.BytesIO()
                            ds.save_as(out_buf)
                            zip_file.writestr(f"instance_{idx}.dcm", out_buf.getvalue())
                        except:
                            zip_file.writestr(f"instance_{idx}.dcm", content)
            
        yield zip_buffer.getvalue()

    return StreamingResponse(
        generate_zip(), 
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=study_{study_id}.zip"}
    )

@router.get("/by-index/{study_idx}/{series_idx}/{instance_idx}")
async def anonymize_by_index(
    study_idx: int,
    series_idx: int,
    instance_idx: int,
    x_anonymizer_key: str = Header(None)
):
    if not x_anonymizer_key:
        raise HTTPException(status_code=401, detail="Missing X-Anonymizer-Key header")

    ris_url = os.getenv('RIS_API_URL', "http://apiserver:8000/app/api")
    proxy_base_url = os.getenv('PACS_PROXY_URL', "http://pacs-proxy:8080")
    anonymizer_key = os.getenv('ANONYMIZER_API_KEY', "")

    if not anonymizer_key:
        raise HTTPException(status_code=500, detail="ANONYMIZER_API_KEY not configured in ML API")

    async with httpx.AsyncClient() as client:
        ris_res = await client.get(
            f"{ris_url}/referrals/study-by-index/{study_idx}/",
            headers={"X-Anonymizer-Key": x_anonymizer_key}
        )
        if ris_res.status_code != 200:
            raise HTTPException(status_code=ris_res.status_code, detail="Failed to find study in RIS")
        
        ris_data = ris_res.json()
        study_uid = ris_data['study_uid']
        iot_token = ris_data['token']
        proxy_headers = {
            "Authorization": f"Bearer {iot_token}",
            "Accept": "application/dicom"
        }

        # PACS Traversal
        series_res = await client.get(f"{proxy_base_url}/studies/{study_uid}/series", headers=proxy_headers)
        series_list = series_res.json()
        series_uid = series_list[series_idx-1].get("0020000E", {}).get("Value", [""])[0]

        instance_res = await client.get(f"{proxy_base_url}/studies/{study_uid}/series/{series_uid}/instances", headers=proxy_headers)
        instance_list = instance_res.json()
        instance_uid = instance_list[instance_idx-1].get("00080018", {}).get("Value", [""])[0]

        file_res = await client.get(
            f"{proxy_base_url}/studies/{study_uid}/series/{series_uid}/instances/{instance_uid}/frames/1",
            headers=proxy_headers
        )
        
        if file_res.status_code == 200:
            content = file_res.content
            c_type = file_res.headers.get("content-type", "")
            if "multipart/related" in c_type:
                unpacked = unpack_multipart_dicom(content, c_type)
                if unpacked: content = unpacked

            try:
                ds = pydicom.dcmread(io.BytesIO(content), force=True)
                ds = engine.anonymize_dataset(ds)
                out_buf = io.BytesIO()
                ds.save_as(out_buf)
                return Response(content=out_buf.getvalue(), media_type="application/dicom")
            except:
                return Response(content=content, media_type="application/dicom")
