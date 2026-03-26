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
        if not hasattr(ds, 'file_meta'):
            ds.file_meta = pydicom.dataset.FileMetaDataset()
        if transfer_syntax:
            ds.file_meta.TransferSyntaxUID = UID(transfer_syntax)
        elif not hasattr(ds.file_meta, 'TransferSyntaxUID'):
            ds.file_meta.TransferSyntaxUID = pydicom.uid.ExplicitVRLittleEndian
        today = datetime.now().strftime('%Y%m%d')
        ds.StudyDate = ds.SeriesDate = ds.ContentDate = today
        ds.PatientIdentityRemoved = "YES"
        ds.DeidentificationMethod = "DICOM PS3.15 Basic Application Level Confidentiality Profile"
        ds.file_meta.MediaStorageSOPInstanceUID = ds.SOPInstanceUID
        ds.file_meta.MediaStorageSOPClassUID = ds.SOPClassUID
        ds.file_meta.ImplementationClassUID = generate_uid()
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
        if res.status_code != 200: raise HTTPException(status_code=res.status_code)
        return res.json()['token']

def get_pacs_auth_headers(token: Optional[str] = None) -> dict:
    """Helper to construct authentication headers for the PACS proxy."""
    headers = {}
    internal_secret = os.getenv("INTERNAL_AUTH_SHARED_SECRET")
    if internal_secret:
        headers["X-Internal-Secret"] = internal_secret
    elif token:
        headers["Authorization"] = f"Bearer {token}"
    return headers

def parse_transfer_syntax(headers: str) -> Optional[str]:
    m = re.search(r'transfer-syntax=([^;\s\r\n]+)', headers)
    return m.group(1).strip('"\'') if m else None

def clean_dicom_data(content: bytes, content_type: str) -> (bytes, Optional[str]):
    if len(content) > 132 and content[128:132] == b"DICM": return content, None
    boundary = None
    if "boundary=" in content_type:
        match = re.search(r'boundary=([^;]+)', content_type)
        if match: boundary = match.group(1).strip('"').encode()
    if not boundary and content.startswith(b'--'):
        boundary = content.split(b'\n')[0].rstrip(b'\r')[2:]
    if boundary:
        parts = content.split(b'--' + boundary)
        for part in parts:
            clean_part = part.lstrip(b'\r\n').rstrip(b'\r\n--')
            if len(clean_part) < 128: continue
            header_end = clean_part.find(b"\r\n\r\n")
            body_start = header_end + 4 if header_end != -1 else clean_part.find(b"\n\n") + 2
            if header_end != -1 or body_start > 1:
                h_str = clean_part[:header_end].decode('utf-8', errors='ignore')
                body = clean_part[body_start:]
                if "application/dicom" in h_str.lower() or b"DICM" in body[128:132] or b"DICM" in body[:4] or len(body) > 1024:
                    return body, parse_transfer_syntax(h_str)
    return content.strip(b'\r\n '), None

def process_multipart_anonymously(content: bytes, content_type: str) -> bytes:
    match = re.search(r'boundary=([^;]+)', content_type)
    if not match: return content
    boundary = match.group(1).strip('"').encode()
    raw_parts = content.split(b'--' + boundary)
    new_parts = []
    for part in raw_parts:
        clean_part = part.lstrip(b'\r\n').rstrip(b'\r\n--')
        if len(clean_part) < 64: continue
        header_end = clean_part.find(b"\r\n\r\n")
        body_start = header_end + 4 if header_end != -1 else clean_part.find(b"\n\n") + 2
        if body_start < 2:
            new_parts.append(part)
            continue
        h_str = clean_part[:header_end].decode('utf-8', errors='ignore')
        body = clean_part[body_start:]
        ts_uid = parse_transfer_syntax(h_str)
        if "application/dicom" in h_str.lower() or b"DICM" in body[128:132] or b"DICM" in body[:4]:
            try:
                ds = pydicom.dcmread(io.BytesIO(body), force=True)
                ds = engine.anonymize_dataset(ds, transfer_syntax=ts_uid)
                buf = io.BytesIO(); ds.save_as(buf, write_like_original=False); body = buf.getvalue()
            except Exception as e: logger.error(f"Part anonymization failed: {e}")
        new_parts.append(clean_part[:body_start] + body + b"\r\n")
    return b'--' + boundary + b'\r\n' + (b'--' + boundary + b'\r\n').join(new_parts) + b'--' + boundary + b'--\r\n'

async def fetch_and_zip_series(proxy_url: str, study_id: str, series_id: str, token: Optional[str] = None):
    buf = io.BytesIO()
    file_count = 0
    local_engine = AnonymizerEngine()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        auth_headers = get_pacs_auth_headers(token)
        async with httpx.AsyncClient() as client:
            list_url = f"{proxy_url.rstrip('/')}/studies/{study_id}/series/{series_id}/instances"
            list_res = await client.get(list_url, headers={**auth_headers, "Accept": "application/dicom+json"}, timeout=20.0)
            if list_res.status_code != 200: raise HTTPException(status_code=list_res.status_code)
            instances = list_res.json()
            def get_inst_num(x):
                val = (x.get("00200013") or {}).get("Value", [0])[0]
                try: return int(val)
                except: return 0
            instances.sort(key=get_inst_num)
            for idx, inst in enumerate(instances):
                iuid = (inst.get("00080018") or {}).get("Value", [None])[0] or \
                       (inst.get("SOPInstanceUID") or {}).get("Value", [None])[0] or \
                       inst.get("00080018") or inst.get("SOPInstanceUID")
                if not iuid: continue
                f_url = f"{proxy_url.rstrip('/')}/studies/{study_id}/series/{series_id}/instances/{iuid}"
                try:
                    f_res = await client.get(f_url, headers={**auth_headers, "Accept": "application/dicom, multipart/related"}, timeout=30.0)
                    if f_res.status_code != 200: continue
                    raw_data, ts_uid = clean_dicom_data(f_res.content, f_res.headers.get("content-type", ""))
                    try:
                        ds = pydicom.dcmread(io.BytesIO(raw_data), force=True)
                        ds = local_engine.anonymize_dataset(ds, transfer_syntax=ts_uid)
                        o = io.BytesIO(); ds.save_as(o, write_like_original=False)
                        zf.writestr(f"IM_{idx+1:04d}.dcm", o.getvalue())
                        file_count += 1
                    except Exception as e:
                        if b"DICM" in raw_data[128:132] or b"DICM" in raw_data[:4]:
                            zf.writestr(f"IM_{idx+1:04d}.dcm", raw_data)
                            file_count += 1
                except Exception as e: logger.error(f"Error processing {iuid}: {e}")
    return buf.getvalue()

# --- Endpoints ---

@router.get("/{path:path}")
async def anonymize_proxy_path(path: str, request: Request, api_key: Optional[str] = Depends(get_api_key), accept: Optional[str] = Header(None)):
    proxy_base_url = os.getenv('PACS_PROXY_URL', "http://pacs-proxy:8080")
    clean_path = path.rstrip('/')
    target_path = path[len("pacs/"):] if path.startswith("pacs/") else path
    target_url = f"{proxy_base_url.rstrip('/')}/{target_path.lstrip('/')}"
    
    logger.info(f"--- ANONYMIZE PROXY REQUEST ---")
    logger.info(f"Path: {path}")
    logger.info(f"Target URL: {target_url}")
    logger.info(f"Incoming Headers (Keys): {list(request.headers.keys())}")
    
    if accept and "application/zip" in accept and "series" in clean_path:
        logger.info("Detected ZIP request for series")
        sid, serid = extract_study_id(clean_path), extract_series_id(clean_path)
        if sid and serid:
            try:
                token = await get_internal_token(sid) if not os.getenv("INTERNAL_AUTH_SHARED_SECRET") else None
                content = await fetch_and_zip_series(proxy_base_url, sid, serid, token)
                return Response(content=content, media_type="application/zip", headers={"Content-Disposition": f"attachment; filename=series_{serid}.zip"})
            except Exception as e: 
                logger.error(f"ZIP failed: {e}", exc_info=True)

    # Prepare headers for the proxy request
    headers = dict(request.headers)
    headers.pop("host", None)
    
    # CRITICAL: Strip any incoming Authorization headers that might conflict with our internal secret
    if "authorization" in headers:
        logger.warning(f"Removing incoming 'authorization' header to prevent proxy conflict")
        headers.pop("authorization")
    if "Authorization" in headers:
        logger.warning(f"Removing incoming 'Authorization' header to prevent proxy conflict")
        headers.pop("Authorization")

    headers["Accept"] = 'application/dicom, multipart/related; type="application/dicom"'
    
    internal_secret = os.getenv("INTERNAL_AUTH_SHARED_SECRET")
    if internal_secret:
        logger.info("Using INTERNAL_AUTH_SHARED_SECRET for proxy authentication")
        headers["X-Internal-Secret"] = internal_secret
    elif api_key and not headers.get("authorization"):
        sid = extract_study_id(clean_path)
        if sid:
            try: 
                token = await get_internal_token(sid)
                headers["Authorization"] = f"Bearer {token}"
                logger.info(f"Fetched and using internal Bearer token for study {sid}")
            except Exception as e: 
                logger.error(f"Failed to fetch internal token: {e}")

    logger.info(f"Outgoing Headers to Proxy: {list(headers.keys())}")

    async with httpx.AsyncClient() as client:
        try:
            res = await client.get(target_url, headers=headers, params=request.query_params, follow_redirects=True, timeout=60.0)
            logger.info(f"Proxy Response: {res.status_code}")
            
            if res.status_code != 200: 
                logger.warning(f"Proxy returned non-200 status: {res.status_code}. Content: {res.content[:200]}")
                return Response(content=res.content, status_code=res.status_code, media_type=res.headers.get("content-type"))
            
            c_type = res.headers.get("content-type", "")
            logger.info(f"Proxy Content-Type: {c_type}")
            
            # Decide how to process based on content-type and requested path
            if "multipart/related" in c_type:
                # If it's a series request (doesn't contain "instances"), we SHOULD keep it multipart
                # to include all 35 images.
                if "instances" not in clean_path:
                    logger.info("Processing series multipart response (anonymizing all parts)")
                    content = process_multipart_anonymously(res.content, c_type)
                    final_c_type = c_type
                else:
                    # If it's a single instance, try to flatten it to a raw DICOM binary
                    logger.info("Flattening single-instance multipart to raw DICOM")
                    raw_data, ts_uid = clean_dicom_data(res.content, c_type)
                    try:
                        ds = pydicom.dcmread(io.BytesIO(raw_data), force=True)
                        ds = engine.anonymize_dataset(ds, transfer_syntax=ts_uid)
                        buf = io.BytesIO(); ds.save_as(buf, write_like_original=False); content = buf.getvalue()
                        final_c_type = "application/dicom"
                    except: 
                        content = raw_data
                        final_c_type = "application/dicom"
            else:
                # Standard non-multipart response
                raw_data, ts_uid = clean_dicom_data(res.content, c_type)
                if b"DICM" in raw_data[128:132] or b"DICM" in raw_data[:4] or "application/dicom" in c_type:
                    try:
                        logger.info("Anonymizing single DICOM instance (non-multipart)")
                        ds = pydicom.dcmread(io.BytesIO(raw_data), force=True)
                        ds = engine.anonymize_dataset(ds, transfer_syntax=ts_uid)
                        buf = io.BytesIO(); ds.save_as(buf, write_like_original=False); content = buf.getvalue()
                        final_c_type = "application/dicom"
                    except: 
                        content = raw_data
                        final_c_type = "application/dicom"
                else: 
                    logger.info("Returning non-DICOM content as-is")
                    content = raw_data
                    final_c_type = c_type
            
            return Response(content=content, media_type=final_c_type)
        except Exception as e: 
            logger.error(f"Proxy request failed: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

@router.get("/series/{study_id}/{series_id}")
async def anonymize_series_direct(study_id: str, series_id: str, x_iot_token: Optional[str] = Header(None), api_key: Optional[str] = Depends(get_api_key)):
    token = x_iot_token or (await get_internal_token(study_id) if api_key and not os.getenv("INTERNAL_AUTH_SHARED_SECRET") else None)
    proxy_url = os.getenv('PACS_PROXY_URL', "http://pacs-proxy:8080")
    content = await fetch_and_zip_series(proxy_url, study_id, series_id, token)
    return Response(content=content, media_type="application/zip", headers={"Content-Disposition": f"attachment; filename=series_{series_id}.zip"})
