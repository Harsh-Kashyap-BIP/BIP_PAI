from pydantic import BaseModel, UUID4
from typing import Optional, List

class ProjectCreate(BaseModel):
    name: str
    user_id: UUID4
    description: Optional[str] = None
    sheet_link: Optional[str] = None
    no_of_mailbox: int = 1
    response_sheet_link: Optional[str] = None



    emails_per_mailbox: int = 30
    email_per_contact: int = 1
    batch_duration_days: int = 2

    contact_limit_very_small: int = 2
    contact_limit_small_company: int = 3
    contact_limit_medium_company: int = 4
    contact_limit_large_company: int = 5
    contact_limit_enterprise: int = 6

    company_size_very_small_max: int = 10
    company_size_small_max: int = 50
    company_size_medium_max: int = 200
    company_size_large_max: int = 1000
    company_size_enterprise_min: int = 1001

    days_between_contacts: int = 3
    follow_up_cycle_days: int = 7

    target_departments: List[str] = []
    excluded_departments: List[str] = []

    seniority_tier_1: List[str] = []
    seniority_tier_2: List[str] = []
    seniority_tier_3: List[str] = []
    seniority_excluded: List[str] = []

class ProjectResponse(ProjectCreate):
    id: UUID4
