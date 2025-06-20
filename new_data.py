import pandas as pd
import numpy as np
import random

# Set random seed
random.seed(42)
np.random.seed(42)

# Sample values
companies = [f"Company_{i}" for i in range(1, 101)]
email_providers = ["gmail", "outlook", "yahoo", "zoho", "no_provider"]
job_titles = ["CEO", "CTO", "Manager", "Engineer", "Founder", "Intern"]
names = [f"User_{i}" for i in range(1, 1001)]

def generate_email(name, provider):
    if provider == "no_provider":
        return f"{name.lower()}@unknown.com"
    return f"{name.lower()}@{provider}.com"

# Create synthetic rows
data = []
for _ in range(500):
    name = random.choice(names)
    provider = random.choice(email_providers)
    company = random.choice(companies)
    title = random.choice(job_titles)
    emp_count = random.choice([25, 75, 150, 300, 750, 1200])
    priority = round(np.random.uniform(0, 1), 2)

    data.append({
        "Name": name,
        "Email": generate_email(name, provider),
        "Email Providers": provider,
        "Company Name": company,
        "Job Title": title,
        "Number of employess": emp_count,
        "Priority Score": priority
    })

# Convert to DataFrame and save
df = pd.DataFrame(data)
df.to_csv("synthetic_leads.csv", index=False)

print("✅ CSV file 'synthetic_leads.csv' saved successfully.")
