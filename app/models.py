from sqlalchemy import Column, Integer, String, ForeignKey, JSON
from sqlalchemy.orm import relationship
from .database import Base


class Referral(Base):
    __tablename__ = "ris_referral"  # Maps to Django app 'ris', model 'Referral'

    id = Column(Integer, primary_key=True, index=True)
    patient_id = Column(String, index=True)
    patient_firstname = Column(String)
    patient_lastname = Column(String)

    # Relationship: One Referral has many StudyDescriptions
    study_descriptions = relationship("StudyDescription", back_populates="referral")


class StudyDescription(Base):
    __tablename__ = "ris_studydescription"

    id = Column(Integer, primary_key=True, index=True)
    # Django foreign keys typically append '_id' to the field name
    referral_id = Column(Integer, ForeignKey("ris_referral.id"))
    measurements = Column(JSON)
    description = Column(String, nullable=True)

    referral = relationship("Referral", back_populates="study_descriptions")