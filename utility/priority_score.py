from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate
from langchain.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field


class PriorityScore(BaseModel):
    priority_score: int = Field(description="Priority score between 0 to 100")
    reason: str = Field(description="Explanation of the score based on the input context")


parser = PydanticOutputParser(pydantic_object=PriorityScore)
format_instructions = parser.get_format_instructions()

# Scoring Rules Prompt (system)
system_template = """
You are an expert assistant for prioritizing B2B leads for campaign.
Campaign description :{desc}

Your task is to assign a priority score (0-100) and explain your reasoning based on the lead's job title, department, and company size.

Use this logic:

Company Size 1-50:
- Primary Roles: CEO, Founder, Co-Founder, Owner
- Secondary Roles: Director, Head of, VP
- Exclude: Intern, Assistant
- Target Depts: All

Company Size 51-100:
- Primary Roles: CEO, Founder, Co-Founder, VP
- Secondary Roles: Director, Head of, Senior Manager
- Exclude: Analyst, Coordinator
- Target Depts: All

Company Size 101-200:
- Primary Roles: Director, VP, Head of
- Secondary Roles: Senior Manager, Manager
- Exclude: CEO, Founder, Analyst
- Target Depts: Sales, Marketing, Operations, Growth
- Exclude Depts: HR, Legal, Finance, Accounting

Company Size 201-500:
- Primary Roles: Director, Head of, Senior Director
- Secondary Roles: VP, Senior Manager
- Exclude: CEO, President, Analyst
- Target Depts: Sales, Marketing, Operations, Growth
- Exclude Depts: HR, Legal, Finance, Accounting

Company Size 501-1000:
- Primary Roles: Senior Manager, Director, Head of
- Secondary Roles: Manager, Senior Director
- Exclude: VP, CEO, President
- Target Depts: Sales, Marketing, Operations, Growth
- Exclude Depts: HR, Legal, Finance, Accounting

Company Size 1000+:
- Handle via ABM strategy; return low score (0–10) and note that ABM is more appropriate

Use this format:
{format_instructions}
"""

human_template = """
Evaluate the following lead:

Job Title: {job_title}
Seniority : {seniority}
Department: {department}
Industry: {industry}
Company Size: {company_size}
"""

prompt = ChatPromptTemplate.from_messages([
    SystemMessagePromptTemplate.from_template(system_template),
    HumanMessagePromptTemplate.from_template(human_template)
])


async def get_priority_score(job_title: str, seniority:str ,department: str, company_size: str, industry: str, desc:str,openai_api_key: str):
    llm =ChatOpenAI(model_name="gpt-4o-mini", temperature=0.3, openai_api_key=openai_api_key)
    chain = prompt | llm | parser
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        print(f"Calculating priority score.Attempt {attempt}...")
        try:
            result = chain.invoke({
                "job_title": job_title,
                "seniority": seniority,
                "department": department,
                "company_size": company_size,
                "desc":desc,
                "industry": industry,
                "format_instructions": format_instructions
            })
            return result.model_dump(),""
        except Exception as e:
            print(f"[Attempt {attempt}] LLM Error: {e}")

    # Return fallback result if all attempts fail
    return {"priority_score": 0, "reason": ""},"Unable to get priority score after {} attempts.".format(max_attempts)

