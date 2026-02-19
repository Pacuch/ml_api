import os
import io
import httpx
import pydicom
import zipfile
import json
import hashlib
from fastapi import APIRouter, HTTPException, Header, Response
from fastapi.responses import StreamingResponse
from pydicom.uid import generate_uid

router = APIRouter(prefix="/anonymize", tags=["anonymization"])

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

    def anonymize_dataset(self, ds):
        # 1. Remove private tags if configured
        ds.remove_private_tags()

        # 2. Iterate through tags and apply actions from JSON
        for elem in list(ds):
            keyword = elem.keyword
            if not keyword or keyword not in self.rules:
                continue

            action = self.rules[keyword]

            if action == 'X':
                delattr(ds, keyword)
            elif action == 'U':
                if elem.value:
                    if elem.VM > 1:
                        elem.value = [self._generate_consistent_uid(u) for u in elem.value]
                    else:
                        elem.value = self._generate_consistent_uid(elem.value)
            elif action in ['Z', 'D']:
                elem.value = self._get_replacement_value(elem.VR, action)

        # 3. Final mandatory metadata (Following your finalize_metadata logic)
        orig_patient_id = str(ds.get("PatientID", "UNKNOWN"))
        hashed_id = hashlib.sha256((orig_patient_id + self.pepper).encode()).hexdigest()
        
        ds.PatientID = hashed_id
        ds.PatientName = f"{hashed_id[:8]}^Anonym"
        ds.PatientIdentityRemoved = "YES"
        
        return ds

# Global instance
engine = AnonymizerEngine()

# --- Endpoints ---

@router.get("/{study_id}")
async def anonymize_study(
    study_id: str, 
    x_iot_token: str = Header(None)
):
    if not x_iot_token:
        raise HTTPException(status_code=401, detail="Missing X-IOT-Token header")

    proxy_base_url = os.getenv('PACS_PROXY_URL')
    
    async def generate_zip():
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            headers = {"Authorization": f"Bearer {x_iot_token}"}
            
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
                        ds = pydicom.dcmread(io.BytesIO(file_res.content))
                        
                        # USE THE FULL ENGINE LOGIC
                        ds = engine.anonymize_dataset(ds)
                        
                        out_buf = io.BytesIO()
                        ds.save_as(out_buf)
                        zip_file.writestr(f"instance_{idx}.dcm", out_buf.getvalue())
            
        yield zip_buffer.getvalue()

    return StreamingResponse(
        generate_zip(), 
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=study_{study_id}.zip"}
    )

@router.get("/{study_idx}/{series_idx}/{instance_idx}")
async def anonymize_by_index(
    study_idx: int,
    series_idx: int,
    instance_idx: int,
    x_anonymizer_key: str = Header(None)
):
    if not x_anonymizer_key:
        raise HTTPException(status_code=401, detail="Missing X-Anonymizer-Key header")

    ris_url = os.getenv('RIS_API_URL', "")
    proxy_base_url = os.getenv('PACS_PROXY_URL', "")
    anonymizer_key = os.getenv('ANONYMIZER_API_KEY', "")

    if not anonymizer_key:
        raise HTTPException(status_code=500, detail="ANONYMIZER_API_KEY not configured in ML API")
    if not ris_url:
        raise HTTPException(status_code=500, detail="RIS_API_URL not configured in ML API")
    if not proxy_base_url:
        raise HTTPException(status_code=500, detail="PACS_PROXY_URL not configured in ML API")

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
        proxy_headers = {"Authorization": f"Bearer {iot_token}"}

        # PACS Traversal (simplified UIDs fetch)
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
            ds = pydicom.dcmread(io.BytesIO(file_res.content))
            
            # USE THE FULL ENGINE LOGIC
            ds = engine.anonymize_dataset(ds)

            out_buf = io.BytesIO()
            ds.save_as(out_buf)
            return Response(content=out_buf.getvalue(), media_type="application/dicom")
