from exa_py import Exa

# noinspection PyTypeChecker
def get_website_summary(website_url, exa_api_key):
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        print(f"Getting website summary for: {website_url}.Attempt {attempt}...")
        try:
            exa = Exa(api_key=exa_api_key)
            response = exa.get_contents(
                [website_url],
                text=True,
                summary={
                    "query": """You are an expert business analyst. 
                                Given any company name and a brief description or website data, generate a clear, structured company summary with the following format and tone. 
                                Keep it concise, factual, and tailored for professional outreach.  
                                FORMAT TO FOLLOW:  
                                COMPANY: [Company Name] – [Brief description: what the company is, what it does]. 
                                Industry: [Industry name]. 
                                Size/Locations: [Estimated size, revenue or AUM if relevant, and geographic focus or HQ].  
                                SERVICES: [What products/services the company provides]. 
                                Target customers: [Type of clients the company serves]. 
                                Geographic reach: [Where they operate].  
                                BUSINESS: Revenue model: [How the company makes money]. 
                                Key achievements/metrics: [Any measurable accomplishments]. 
                                Recent developments: [Recent fundraising, partnerships, product launches, etc.].  
                                OUTREACH ANGLES:  
                                Challenge: [A possible problem or opportunity the company might be facing].  
                                Growth: [How your solution can help them grow, scale, or optimize].  
                                Advantage: [A unique strength you/your firm offers that fits their goals].  
                                Make sure all information is fact-based, and infer only when context clearly allows. 
                                Use a confident, advisory tone, suitable for B2B strategy or consulting outreach."""
                                                }
                                            )
            summary = response.results[0].summary
            if "COMPANY" in summary:
                return summary, ""
            else:
                print("Summary not in the required format'. Retrying...")
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")

    return "", f"Failed to get a valid summary after {max_attempts} attempts."

