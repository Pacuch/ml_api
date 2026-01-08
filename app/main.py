from fastapi import FastAPI
from .routers import measurements # Import the new router

app = FastAPI()

app.include_router(measurements.router)

@app.get("/")
def health_check():
    return {"status": "ok", "service": "Measurement Data API"}
