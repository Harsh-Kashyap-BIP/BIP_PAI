from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate
from langchain.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field
from typing import Optional

# Define expected input schema
class InputColumns(BaseModel):
    first_name: Optional[str] = Field(description="First name")
    last_name: Optional[str] = Field(description="Last name")
    company_name: Optional[str] = Field(description="Company name")
    email: Optional[str] = Field(description="Email address")
    job_title: Optional[str] = Field(description="Job title")
    seniority: Optional[str] = Field(description="Seniority")
    industry: Optional[str] = Field(description="Industry")
    department: Optional[str] = Field(description="Department")
    company_website: Optional[str] = Field(description="Company website")
    company_linkedin: Optional[str] = Field(description="Company LinkedIn URL")
    employee_count: Optional[str] = Field(description="Employee count at the company")

# Parser and instructions
parser = PydanticOutputParser(pydantic_object=InputColumns)
format_instructions = parser.get_format_instructions()

# Prompt templates
system_template = """
You are a helpful assistant that maps a list of raw column names to a standardized schema.

Your task is to return a JSON object with the following keys:
[first_name, last_name, company_name, email, job_title, seniority,industry, company_website, company_linkedin, employee_count]

Each value should be the closest matching column name (case-insensitive match or synonyms) from the provided list.
If no suitable match is found for a key, return null for that field.

Use this format:
{format_instructions}
"""

human_template = """
Here is the list of column names provided by the user:
{user_columns}
"""

# Final prompt template
prompt = ChatPromptTemplate.from_messages([
    SystemMessagePromptTemplate.from_template(system_template),
    HumanMessagePromptTemplate.from_template(human_template)
])

# Retry-able column name mapping function
async def get_column_names(user_column_names: list, openai_api_key: str) -> dict:
    llm = ChatOpenAI(model_name="gpt-4o-mini", temperature=0.0, openai_api_key=openai_api_key)
    chain = prompt | llm | parser
    max_attempts = 3
    for attempt in range(1,max_attempts+1):
        print(f'Getting column names.Attempt #{attempt}')
        try:
            result = chain.invoke({
                "user_columns": ", ".join(user_column_names),
                "format_instructions": format_instructions
            })
            return result.model_dump()
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")

    # If all retries fail, return default structure with None values
    return {field: None for field in InputColumns.model_fields.keys()}

