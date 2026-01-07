import os

# Configuration Constants
API_KEY_NAME = "X-API-Key"
STATUS_SIGNED = 7

# Environment Variables
SECRET_PEPPER = os.getenv("SECRET_PEPPER", "default_insecure_pepper")
SERVER_API_KEY = os.getenv("API_KEY", "unsafe_secret")