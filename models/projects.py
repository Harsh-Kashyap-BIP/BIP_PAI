from sqlalchemy import Column, String, Integer, Text
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from uuid import uuid4
from database.base import Base

class Project(Base):
    __tablename__ = "projects"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    name = Column(String, nullable=False)
    user_id = Column(UUID(as_uuid=True), nullable=False)
    description = Column(Text)
    sheet_link = Column(String)
    no_of_mailbox = Column(Integer)
    response_sheet_link = Column(String)

    emails_per_mailbox = Column(Integer)
    email_per_contact = Column(Integer)
    batch_duration_days = Column(Integer)

    contact_limit_very_small = Column(Integer)
    contact_limit_small_company = Column(Integer)
    contact_limit_medium_company = Column(Integer)
    contact_limit_large_company = Column(Integer)
    contact_limit_enterprise = Column(Integer)

    company_size_very_small_max = Column(Integer)
    company_size_small_max = Column(Integer)
    company_size_medium_max = Column(Integer)
    company_size_large_max = Column(Integer)
    company_size_enterprise_min = Column(Integer)

    days_between_contacts = Column(Integer)
    follow_up_cycle_days = Column(Integer)

    target_departments = Column(ARRAY(String))
    excluded_departments = Column(ARRAY(String))

    seniority_tier_1 = Column(ARRAY(String))
    seniority_tier_2 = Column(ARRAY(String))
    seniority_tier_3 = Column(ARRAY(String))
    seniority_excluded = Column(ARRAY(String))
