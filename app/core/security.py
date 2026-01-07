import hashlib
from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader
from .config import SERVER_API_KEY, SECRET_PEPPER, API_KEY_NAME

# Setup the header scheme
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_api_key(api_key_header: str = Security(api_key_header)):
    """
    Validates the API Key from the header.
    """
    if api_key_header == SERVER_API_KEY:
        return api_key_header

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
    )

def hash_patient_id(original_id: str) -> str:
    """Hashes the patient ID with a secret pepper."""
    if not original_id:
        return ""
    combined_string = str(original_id) + SECRET_PEPPER
    return hashlib.sha256(combined_string.encode('utf-8')).hexdigest()