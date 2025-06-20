from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.security import HTTPBearer,HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from jose import JWTError, jwt
from pydantic import BaseModel,Field
import httpx
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncSession
from crud.projects import get_projects_id, get_project_by_id, create_project
from database.config import get_db
from uuid import UUID

from personalized import generate_personalized_sheet
from schema.projects import ProjectResponse, ProjectCreate
from upload_file_superbase import upload_df_to_supabase_async
from utility.column_names import get_column_names
from utility.google_sheet_handeling import get_google_sheet_as_dataframe

load_dotenv()

# ------------------- Config -------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
JWT_SECRET = os.getenv("JWT_SECRET")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_MINUTES = 1440

app = FastAPI(
    title="Personalized AI",
    description="Personalized AI",
    version="0.1.0",
    docs_url="/")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ------------------- Models -------------------
class SignupRequest(BaseModel):
    email: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    uuid: str

# ------------------- JWT Helpers -------------------
auth_scheme = HTTPBearer()
def create_jwt_token(data: dict, expires_delta: timedelta = timedelta(minutes=JWT_EXPIRATION_MINUTES)):
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM)


async def verify_token(token: HTTPAuthorizationCredentials = Depends(auth_scheme)):
    try:
        jwt_token = token.credentials
        payload = jwt.decode(jwt_token, JWT_SECRET, algorithms=[JWT_ALGORITHM])

        email = payload.get("sub")
        uuid = payload.get("uuid")

        if not email or not uuid:
            raise HTTPException(status_code=401, detail="Invalid token")

        return {"email": email, "uuid": uuid}
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

# ------------------- Auth Endpoints -------------------
@app.post("/signup")
async def signup(user: SignupRequest):
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{SUPABASE_URL}/auth/v1/signup",
            headers={"apikey": SUPABASE_KEY, "Content-Type": "application/json"},
            json={"email": user.email, "password": user.password}
        )

    if response.status_code == 200:
        return {"message": "Signup successful. Check your email for confirmation."}

    error_data = response.json()
    error_msg = error_data.get("msg", "").lower()

    if "already registered" in error_msg:
        raise HTTPException(status_code=400, detail="Email is already registered.")
    elif "password" in error_msg:
        raise HTTPException(status_code=400, detail="Password is too weak.")

    raise HTTPException(status_code=response.status_code, detail=error_data.get("msg", "Signup failed."))


@app.post("/login", response_model=TokenResponse)
async def login(user: SignupRequest):
    async with httpx.AsyncClient() as client:
        # Step 1: Login to Supabase
        response = await client.post(
            f"{SUPABASE_URL}/auth/v1/token?grant_type=password",
            headers={"apikey": SUPABASE_KEY, "Content-Type": "application/json"},
            json={"email": user.email, "password": user.password}
        )

        if response.status_code != 200:
            error_data = response.json()
            error_msg = error_data.get("error_description", "").lower()

            if "invalid login credentials" in error_msg:
                raise HTTPException(status_code=401, detail="Incorrect email or password.")
            elif "email not confirmed" in error_msg:
                raise HTTPException(status_code=401, detail="Email not confirmed. Please verify your email.")
            raise HTTPException(status_code=response.status_code, detail=error_data.get("error_description", "Login failed."))

        access_token = response.json()["access_token"]

        # Step 2: Get the user's UUID using the access token
        user_response = await client.get(
            f"{SUPABASE_URL}/auth/v1/user",
            headers={"Authorization": f"Bearer {access_token}", "apikey": SUPABASE_KEY}
        )

        if user_response.status_code != 200:
            raise HTTPException(status_code=500, detail="Failed to fetch user details from Supabase.")

        user_data = user_response.json()
        user_id = user_data.get("id")  # UUID

        if not user_id:
            raise HTTPException(status_code=500, detail="User ID not found in Supabase response.")

        # Step 3: Create and return custom JWT with UUID
        jwt_token = create_jwt_token({"sub": user.email, "uuid": user_id})
        return TokenResponse(access_token=jwt_token, uuid=user_id)


@app.post("/logout")
async def logout(request: Request):
    return {"message": "Logout handled on client by deleting token"}
# ------------------- Protected Endpoints -------------------

class googleSheetRequest(BaseModel):
    project_id:str = Field(description="Project ID")
    original_sheet_url: str=Field(description="Google Sheet URL")
    proceed_on_invalid_email:bool=Field(False,description="Proceed with invalid email")
    openai_key:str=Field(description="OpenAI API key")
    ss_masters_key: str = Field(description="SSMASTERS API key")
    exa_api_key:str=Field(description="Exa AI API key")

class googleSheetResponse(BaseModel):
    sheet_link: str=Field(description="Google sheet link")

@app.post("/personalized-sheet",response_model=googleSheetResponse)
async def google_sheet(request:googleSheetRequest,user=Depends(verify_token)):
    # Getting the google-sheet data
    try:
        data= await get_google_sheet_as_dataframe(request.original_sheet_url)
        if data is None or data.empty:
            raise HTTPException(
                status_code=400,
                detail="No data retrieved from Google Sheet or sheet is empty"
            )
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to access Google Sheet: {str(e)}"
        )
    # Extracting column names
    column_names = await get_column_names(user_column_names=data.columns.tolist(),
                                              openai_api_key=request.openai_key)
    # Check for missing mappings
    missing_keys = [key for key, value in column_names.items() if value is None]
    # Raise exception if any required mapping is missing
    if missing_keys:
        raise HTTPException(
            status_code=400,
            detail=f"The following required columns were not found in the uploaded table: {', '.join(missing_keys)}"
            )
    personalized_sheet= await generate_personalized_sheet(data=data.head(100),request=request,column_names=column_names)
    public_url= await upload_df_to_supabase_async(df=personalized_sheet,file_prefix=f'{user["uuid"]}_sheet')
    return {
        "sheet_link": public_url
    }

@app.get("/projects/{user_id}")
async def list_project_ids(user_id: UUID, db: AsyncSession = Depends(get_db),user=Depends(verify_token)):
    return await get_projects_id(db, user_id)

@app.get("/project/{project_id}")
async def get_project(project_id: UUID, db: AsyncSession = Depends(get_db),user=Depends(verify_token)):
    project = await get_project_by_id(db, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project

@app.post("/project", response_model=ProjectResponse)
async def create_project_endpoint(
    project: ProjectCreate,
    db: AsyncSession = Depends(get_db),
    user=Depends(verify_token)
):
    return await create_project(db, project)

