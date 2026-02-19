from fastapi import FastAPI
from .routers import measurements, anonymization # Import the new routers

app = FastAPI()

app.include_router(measurements.router)
app.include_router(anonymization.router)

@app.get("/")
def health_check():
    return {"status": "ok", "service": "Measurement Data API"}
