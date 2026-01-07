from sqlalchemy.orm import Session
from sqlalchemy import desc
from . import models

# 1. Get a specific study by its ID (The "Select" part)
def get_referral_by_study_id(db: Session, study_id: str):
    return db.query(models.Referral)\
             .filter(models.Referral.study_id == study_id)\
             .first()

# 2. Get all studies (The "Read all" part)
# We use skip/limit for pagination so we don't crash the server with 100k rows
def get_all_referrals(db: Session, skip: int = 0, limit: int = 100, status: int = None):
    query = db.query(models.Referral)

    # Apply filter if status is provided
    if status is not None:
        query = query.filter(models.Referral.status == status)

    return query.order_by(desc(models.Referral.study_datetime)) \
        .offset(skip) \
        .limit(limit) \
        .all()