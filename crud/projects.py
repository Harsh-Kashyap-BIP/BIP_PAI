from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from typing import List, Optional
from models.projects import Project
from uuid import UUID
from schema.projects import ProjectCreate


# Get list of project IDs by user_id
async def get_projects_id(session: AsyncSession, user_id: UUID) -> List[UUID]:
    result = await session.execute(
        select(Project.id).where(Project.user_id == user_id)
    )
    return [row[0] for row in result.all()]


# Get full project details by project_id
async def get_project_by_id(session: AsyncSession, project_id: UUID) -> Optional[Project]:
    result = await session.execute(
        select(Project).where(Project.id == project_id)
    )
    return result.scalars().first()

# Create a new project
async def create_project(session: AsyncSession, project_data: ProjectCreate) -> Project:
    new_project = Project(**project_data.dict())
    session.add(new_project)
    await session.commit()
    await session.refresh(new_project)
    return new_project
