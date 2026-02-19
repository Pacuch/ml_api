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
def get_all_referrals(db: Session, skip: int = 0, limit: int = 100, min_status: int = None):
    query = db.query(models.Referral)

    if min_status is not None:
        # Started (5), Saved (6), Signed (7)
        query = query.filter(models.Referral.status > min_status)


    return query.order_by(models.Referral.id) \
        .offset(skip) \
        .limit(limit) \
        .all()
