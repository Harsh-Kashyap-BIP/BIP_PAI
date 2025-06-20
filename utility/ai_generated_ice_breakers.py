from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate
from langchain.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field

# Define Pydantic Output Schema
class ColdLiners(BaseModel):
    option1: str = Field(description="First variation focusing on scale/operations")
    option2: str = Field(description="Second variation focusing on strategic positioning")
    option3: str = Field(description="Third variation focusing on execution/results")
    selected: str = Field(description="Best option that shows most strategic understanding")
    reason: str = Field(description="Why this demonstrates deepest industry knowledge")

# Create the parser
parser = PydanticOutputParser(pydantic_object=ColdLiners)

# Define the system prompt
system_template = """
# ROLE
You are an expert at writing authentic peer-to-peer compliments for B2B outreach that sound like genuine industry recognition.

# CORE RULES
- Write like a knowledgeable industry peer giving sincere recognition
- Use casual, conversational tone - no formal sales language
- Combine multiple impressive facts into one cohesive observation
- Focus on operational excellence, strategic positioning, or market impact
- Keep under 20 words total
- NO sales questions, asks, or hints
- NO exclamation marks - sound confident, not excited
- Don't start with "I" or use first person

# CRITICAL: FACT-ONLY RULE
- ONLY use explicit facts from the provided summary
- NEVER add numbers, percentages, locations, or achievements not mentioned
- If no specifics given, focus on general strategic observations

# TONE EXAMPLES
- "Building across 8 states with consistent quality and competitive rates - that's operational excellence"
- "Multi-market expansion while keeping agent quality high - that's rare in real estate"
- "Quick move-in inventory strategy across multiple states - you're solving real buyer pain points"
"""

# Define the human/user prompt
human_template = """
Based on this company summary:

Website Summary:
{website_summary}

LinkedIn Summary:
{linkedin_summary}

Create 3 peer-to-peer compliment variations that combine their most impressive facts into single observations.

STRUCTURE: [Strategic insight] + [specific accomplishment] + [Use humanised company name somewhere appropriately in the sentence] + [appreciation ending]

## Focus on:
- Operational scale/complexity they've mastered
- Smart strategic positioning in their market
- Impressive execution across multiple dimensions

## Since this is for AI lead qualification services, prioritize compliments about:
- Multi-location operations
- Customer experience improvements
- Growth/expansion achievements
- Innovative approaches to traditional problems

## Humanization of Company Name
- Simplify the company name to be referred in the email with ease
- Shorten it like how a human would do with in the company
- Keep the length max to 1-2 words as much as possible

{format_instructions}
"""

# Combine into a prompt template
prompt = ChatPromptTemplate.from_messages([
    SystemMessagePromptTemplate.from_template(system_template),
    HumanMessagePromptTemplate.from_template(human_template)
])

# Final chain function
async def generate_ice_breakers_chain(website_summary, linkedin_summary, openai_api_key):
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        print(f"Generating cold liners...Attempt {attempt}...")
        try:
            llm = ChatOpenAI(model_name="gpt-4o-mini", temperature=0.7, openai_api_key=openai_api_key)
            chain = prompt | llm | parser
            result = chain.invoke({
                "website_summary": website_summary,
                "linkedin_summary": linkedin_summary,
                "format_instructions": parser.get_format_instructions()
            })
            if result:
                response=result.model_dump()
                return (f"1.{response['option1']} \n 2.{response['option2']} \n 3.{response['option3']}",
                        f"{response[response['selected']]}",
                        f"{response['reason']}",
                        "")
            else:
                print("Empty result received. Retrying...")

        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")
            if attempt == max_attempts:
                return "","","",f"Unable to get ice breakers:{e}"

    return "","","","Internal Server Error"