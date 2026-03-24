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
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Header, Response, Request, Depends
from fastapi.responses import StreamingResponse
from pydicom.uid import generate_uid

from ..core.security import get_api_key

router = APIRouter(prefix="/anonym", tags=["anonymization"])
logger = logging.getLogger("uvicorn")

# --- Core Anonymization Engine ---

class AnonymizerEngine:
    def __init__(self):
        self.rules = self._load_rules()
        self.pepper = os.getenv("PEPPER", "default_secret_pepper")
        self.uid_map = {}

    def _load_rules(self):
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
        except: pass

        for elem in list(dataset):
            keyword = elem.keyword
            if not keyword: continue

            if elem.VR == 'SQ':
                if keyword in self.rules and self.rules[keyword] == 'X':
                    delattr(dataset, keyword)
                else:
                    for item in elem.value: 
                        self._process_dataset_recursive(item)
                continue

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
        self._process_dataset_recursive(ds)
        today = datetime.now().strftime('%Y%m%d')
        ds.StudyDate = ds.SeriesDate = ds.ContentDate = today
        ds.PatientIdentityRemoved = "YES"
        ds.DeidentificationMethod = "DICOM PS3.15 Basic Application Level Confidentiality Profile"
        return ds

engine = AnonymizerEngine()

# --- Helpers ---

def extract_study_id(path: str) -> Optional[str]:
    match = re.search(r'studies/([^/]+)', path)
    return match.group(1) if match else None

async def get_internal_token(study_id: str) -> str:
    ris_url = os.getenv('RIS_API_URL', "http://apiserver:8000/app/api")
    akey = os.getenv('ANONYMIZER_API_KEY', "")
    async with httpx.AsyncClient() as client:
        res = await client.get(f"{ris_url}/referrals/study-by-uid/{study_id}/", headers={"X-Anonymizer-Key": akey})
        if res.status_code != 200: raise HTTPException(status_code=res.status_code)
        return res.json()['token']

def process_multipart_anonymously(content: bytes, content_type: str) -> bytes:
    """
    Parses a multipart/related stream, anonymizes every DICOM part, and reassembles it.
    """
    match = re.search(r'boundary=([^;]+)', content_type)
    if not match: return content
    
    boundary = match.group(1).strip('"').encode()
    # Parts are separated by --boundary
    raw_parts = content.split(b'--' + boundary)
    
    new_parts = []
    for part in raw_parts:
        if not part or part.strip() == b'--': continue
        
        header_end = part.find(b"\r\n\r\n")
        if header_end == -1:
            new_parts.append(part)
            continue
            
        headers = part[:header_end+4]
        body = part[header_end+4:].rstrip(b"\r\n")
        
        # Check if this part is DICOM (by header or content)
        is_dcm = b"application/dicom" in headers or b"DICM" in body[128:132]
        
        if is_dcm:
            try:
                ds = pydicom.dcmread(io.BytesIO(body), force=True)
                ds = engine.anonymize_dataset(ds)
                buf = io.BytesIO()
                ds.save_as(buf)
                body = buf.getvalue()
            except Exception as e:
                logger.error(f"Failed to anonymize part in multipart: {e}")
        
        new_parts.append(headers + body + b"\r\n")
        
    return b'--' + boundary + b'\r\n' + (b'--' + boundary + b'\r\n').join(new_parts) + b'--' + boundary + b'--\r\n'

# --- Endpoints ---

@router.get("/{path:path}")
async def anonymize_proxy_path(path: str, request: Request, api_key: Optional[str] = Depends(get_api_key)):
    proxy_base_url = os.getenv('PACS_PROXY_URL', "http://pacs-proxy:8080")
    target_path = path[len("pacs/"):] if path.startswith("pacs/") else path
    target_url = f"{proxy_base_url.rstrip('/')}/{target_path.lstrip('/')}"
    
    headers = dict(request.headers)
    headers.pop("host", None)
    headers["Accept"] = 'application/dicom, multipart/related; type="application/dicom"'

    if api_key and not headers.get("authorization"):
        sid = extract_study_id(path)
        if sid:
            try: headers["Authorization"] = f"Bearer {await get_internal_token(sid)}"
            except: pass

    async with httpx.AsyncClient() as client:
        try:
            res = await client.get(target_url, headers=headers, params=request.query_params, follow_redirects=True, timeout=60.0)
            if res.status_code != 200:
                return Response(content=res.content, status_code=res.status_code, media_type=res.headers.get("content-type"))
            
            c_type = res.headers.get("content-type", "")
            content = res.content
            
            if "multipart/related" in c_type:
                content = process_multipart_anonymously(content, c_type)
            else:
                is_dicom = "application/dicom" in c_type or b"DICM" in content[128:132]
                if is_dicom:
                    try:
                        ds = pydicom.dcmread(io.BytesIO(content), force=True)
                        ds = engine.anonymize_dataset(ds)
                        buf = io.BytesIO(); ds.save_as(buf)
                        content = buf.getvalue()
                    except: pass
            
            return Response(content=content, media_type=c_type)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@router.get("/series/{study_id}/{series_id}")
async def anonymize_series_zip(study_id: str, series_id: str, x_iot_token: Optional[str] = Header(None), api_key: Optional[str] = Depends(get_api_key)):
    token = x_iot_token or (await get_internal_token(study_id) if api_key else None)
    if not token: raise HTTPException(status_code=401)
    proxy_url = os.getenv('PACS_PROXY_URL', "http://pacs-proxy:8080")
    
    async def generate():
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/dicom"}
            async with httpx.AsyncClient() as client:
                instances = (await client.get(f"{proxy_url}/studies/{study_id}/series/{series_id}/instances", headers=headers)).json()
                for idx, inst in enumerate(instances):
                    iuid = inst.get("00080018", {}).get("Value", [""])[0]
                    f_res = await client.get(f"{proxy_url}/studies/{study_id}/series/{series_id}/instances/{iuid}/frames/1", headers=headers)
                    if f_res.status_code == 200:
                        data = f_res.content
                        if "multipart/related" in f_res.headers.get("content-type", ""):
                            # Use existing logic to strip multipart for the ZIP
                            match = re.search(r'boundary=([^;]+)', f_res.headers.get("content-type"))
                            if match:
                                boundary = match.group(1).strip('"').encode()
                                parts = data.split(b'--' + boundary)
                                for p in parts:
                                    if b"DICM" in p[128:132] or b"application/dicom" in p:
                                        data = p[p.find(b"\r\n\r\n")+4:].rstrip(b"\r\n--")
                                        break
                        try:
                            ds = pydicom.dcmread(io.BytesIO(data), force=True)
                            ds = engine.anonymize_dataset(ds)
                            o = io.BytesIO(); ds.save_as(o)
                            zf.writestr(f"instance_{idx}.dcm", o.getvalue())
                        except: zf.writestr(f"instance_{idx}.dcm", data)
        yield buf.getvalue()
    return StreamingResponse(generate(), media_type="application/zip", headers={"Content-Disposition": f"attachment; filename=series_{series_id}.zip"})
