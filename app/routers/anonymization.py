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
from pydicom.uid import generate_uid, UID

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
            logger.error(f"Could not load anonymization config: {e}")
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

    def anonymize_dataset(self, ds, transfer_syntax=None):
        self._process_dataset_recursive(ds)
        
        # Ensure transfer syntax is preserved if provided from headers
        if transfer_syntax:
            if not hasattr(ds, 'file_meta'):
                ds.file_meta = pydicom.dataset.FileMetaDataset()
            ds.file_meta.TransferSyntaxUID = UID(transfer_syntax)
            
        today = datetime.now().strftime('%Y%m%d')
        ds.StudyDate = ds.SeriesDate = ds.ContentDate = today
        ds.PatientIdentityRemoved = "YES"
        ds.DeidentificationMethod = "DICOM PS3.15 Basic Application Level Confidentiality Profile"
        return ds

engine = AnonymizerEngine()

# --- Helpers ---

def extract_study_id(path: str) -> Optional[str]:
    match = re.search(r'studies/([^/]+)', path)
    return match.group(1).rstrip('/') if match else None

def extract_series_id(path: str) -> Optional[str]:
    match = re.search(r'series/([^/]+)', path)
    return match.group(1).rstrip('/') if match else None

async def get_internal_token(study_id: str) -> str:
    ris_url = os.getenv('RIS_API_URL', "http://apiserver:8000/app/api")
    akey = os.getenv('ANONYMIZER_API_KEY', "")
    async with httpx.AsyncClient() as client:
        res = await client.get(f"{ris_url}/referrals/study-by-uid/{study_id}/", headers={"X-Anonymizer-Key": akey})
        if res.status_code != 200: 
            logger.error(f"RIS token fetch failed for {study_id}: {res.status_code}")
            raise HTTPException(status_code=res.status_code)
        return res.json()['token']

def parse_transfer_syntax(headers: str) -> Optional[str]:
    """Helper to find transfer-syntax in MIME headers."""
    m = re.search(r'transfer-syntax=([^;\s\r\n]+)', headers)
    if m:
        return m.group(1).strip('"\'')
    return None

def process_multipart_anonymously(content: bytes, content_type: str) -> bytes:
    match = re.search(r'boundary=([^;]+)', content_type)
    if not match: return content
    boundary = match.group(1).strip('"').encode()
    raw_parts = content.split(b'--' + boundary)
    
    new_parts = []
    for part in raw_parts:
        # THE FIX: Strip leading/trailing newlines to prevent offset bugs
        clean_part = part.lstrip(b'\r\n').rstrip(b'\r\n--')
        if len(clean_part) < 64: continue
        
        header_end = clean_part.find(b"\r\n\r\n")
        if header_end == -1:
            new_parts.append(part)
            continue
            
        headers_str = clean_part[:header_end].decode('utf-8', errors='ignore')
        body = clean_part[header_end+4:]
        
        ts_uid = parse_transfer_syntax(headers_str)
        is_dcm = "application/dicom" in headers_str or b"DICM" in body[128:132] or b"DICM" in body[:4]
        
        if is_dcm:
            try:
                # THE FIX: force=True to handle missing preambles
                ds = pydicom.dcmread(io.BytesIO(body), force=True)
                ds = engine.anonymize_dataset(ds, transfer_syntax=ts_uid)
                buf = io.BytesIO(); ds.save_as(buf); body = buf.getvalue()
            except Exception as e:
                logger.error(f"Multipart anonymization failed: {e}")
        
        new_parts.append(clean_part[:header_end+4] + body + b"\r\n")
        
    return b'--' + boundary + b'\r\n' + (b'--' + boundary + b'\r\n').join(new_parts) + b'--' + boundary + b'--\r\n'

async def fetch_and_zip_series(proxy_url: str, study_id: str, series_id: str, token: str):
    buf = io.BytesIO()
    file_count = 0
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        auth_header = {"Authorization": f"Bearer {token}"}
        async with httpx.AsyncClient() as client:
            list_url = f"{proxy_url.rstrip('/')}/studies/{study_id}/series/{series_id}/instances"
            list_res = await client.get(list_url, headers={**auth_header, "Accept": "application/dicom+json"}, timeout=20.0)
            if list_res.status_code != 200: raise HTTPException(status_code=list_res.status_code)
                
            instances = list_res.json()
            for idx, inst in enumerate(instances):
                # Robust UID extraction
                iuid = (inst.get("00080018") or {}).get("Value", [None])[0] or inst.get("SOPInstanceUID", {}).get("Value", [None])[0]
                if not iuid: continue
                
                # Fetching the instance (WADO-RS)
                f_url = f"{proxy_url.rstrip('/')}/studies/{study_id}/series/{series_id}/instances/{iuid}"
                f_res = await client.get(f_url, headers={**auth_header, "Accept": "application/dicom, multipart/related"}, timeout=30.0)
                
                if f_res.status_code == 200:
                    data, c_type = f_res.content, f_res.headers.get("content-type", "")
                    ts_uid = None
                    
                    if "multipart/related" in c_type:
                        match = re.search(r'boundary=([^;]+)', c_type)
                        if match:
                            b = match.group(1).strip('"').encode()
                            parts = data.split(b'--' + b)
                            for p in parts:
                                clean_p = p.lstrip(b'\r\n').rstrip(b'\r\n--')
                                if len(clean_p) < 64: continue
                                h_end = clean_p.find(b"\r\n\r\n")
                                if h_end != -1:
                                    h_str = clean_p[:h_end].decode('utf-8', errors='ignore')
                                    p_body = clean_p[h_end+4:]
                                    if "application/dicom" in h_str or b"DICM" in p_body[128:132] or b"DICM" in p_body[:4]:
                                        data, ts_uid = p_body, parse_transfer_syntax(h_str)
                                        break
                    try:
                        ds = pydicom.dcmread(io.BytesIO(data), force=True)
                        ds = engine.anonymize_dataset(ds, transfer_syntax=ts_uid)
                        o = io.BytesIO(); ds.save_as(o); data = o.getvalue()
                    except Exception as e:
                        logger.warning(f"Zip anonymization failed for {iuid}: {e}")
                    
                    zf.writestr(f"IM_{idx+1:04d}.dcm", data)
                    file_count += 1
    
    logger.info(f"DEBUG: ZIP created with {file_count} files")
    return buf.getvalue()

# --- Endpoints ---

@router.get("/{path:path}")
async def anonymize_proxy_path(path: str, request: Request, api_key: Optional[str] = Depends(get_api_key), accept: Optional[str] = Header(None)):
    proxy_base_url = os.getenv('PACS_PROXY_URL', "http://pacs-proxy:8080")
    clean_path = path.rstrip('/')
    target_path = path[len("pacs/"):] if path.startswith("pacs/") else path
    target_url = f"{proxy_base_url.rstrip('/')}/{target_path.lstrip('/')}"
    
    if accept and "application/zip" in accept and "series" in clean_path:
        sid, serid = extract_study_id(clean_path), extract_series_id(clean_path)
        if sid and serid:
            try:
                token = await get_internal_token(sid)
                content = await fetch_and_zip_series(proxy_base_url, sid, serid, token)
                return Response(content=content, media_type="application/zip", headers={"Content-Disposition": f"attachment; filename=series_{serid}.zip"})
            except Exception as e: logger.error(f"ZIP failed: {e}")

    headers = dict(request.headers); headers.pop("host", None)
    headers["Accept"] = 'application/dicom, multipart/related; type="application/dicom"'
    if api_key and not headers.get("authorization"):
        sid = extract_study_id(clean_path)
        if sid:
            try: headers["Authorization"] = f"Bearer {await get_internal_token(sid)}"
            except: pass

    async with httpx.AsyncClient() as client:
        try:
            res = await client.get(target_url, headers=headers, params=request.query_params, follow_redirects=True, timeout=60.0)
            if res.status_code != 200: return Response(content=res.content, status_code=res.status_code, media_type=res.headers.get("content-type"))
            c_type, content = res.headers.get("content-type", ""), res.content
            if "multipart/related" in c_type: content = process_multipart_anonymously(content, c_type)
            else:
                is_dicom = "application/dicom" in c_type or b"DICM" in content[128:132] or b"DICM" in content[:4]
                if is_dicom:
                    try:
                        ds = pydicom.dcmread(io.BytesIO(content), force=True)
                        ds = engine.anonymize_dataset(ds)
                        buf = io.BytesIO(); ds.save_as(buf); content = buf.getvalue()
                    except: pass
            return Response(content=content, media_type=c_type)
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@router.get("/series/{study_id}/{series_id}")
async def anonymize_series_direct(study_id: str, series_id: str, x_iot_token: Optional[str] = Header(None), api_key: Optional[str] = Depends(get_api_key)):
    token = x_iot_token or (await get_internal_token(study_id) if api_key else None)
    if not token: raise HTTPException(status_code=401)
    proxy_url = os.getenv('PACS_PROXY_URL', "http://pacs-proxy:8080")
    content = await fetch_and_zip_series(proxy_url, study_id, series_id, token)
    return Response(content=content, media_type="application/zip", headers={"Content-Disposition": f"attachment; filename=series_{series_id}.zip"})
