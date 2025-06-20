import requests

# noinspection PyTypeChecker
def get_company_linkedin_data(linkedin_url, ss_masters_api_key):

    url = "https://commande-center.p.rapidapi.com/linkedin-company-info"
    payload = {"url": linkedin_url}
    headers = {
        "x-rapidapi-key": ss_masters_api_key,
        "x-rapidapi-host": "commande-center.p.rapidapi.com",
        "Content-Type": "application/json"
    }

    max_attempts = 2

    for attempt in range(1, max_attempts + 1):
        print(f"Fetching LinkedIn data for: {linkedin_url}.Attempt {attempt}...")
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            result = response.json()[0]

            description = result.get("Company Info", {}).get("Company Description", "")
            employees = result.get("Company Info", {}).get("Number of Employees", "")

            if description and employees:
                return description, employees, ""
            else:
                print("Incomplete data received. Retrying...")

        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")

    return "-", "-", f"Unable to get LinkedIn data after {max_attempts} attempts."
