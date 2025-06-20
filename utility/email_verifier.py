import requests

async def lead_email_verifier(email, api_key):
    url = "https://commande-center.p.rapidapi.com/email-verifier"
    querystring = {"email": email}
    headers = {
        "x-rapidapi-key": api_key,  # use the parameter passed instead of hardcoding
        "x-rapidapi-host": "commande-center.p.rapidapi.com"
    }
    max_attempts=3
    # noinspection PyTypeChecker
    for attempt in range(1,max_attempts+1):  # Max 3 attempts
        print(f"Verifying email: {email} (Attempt {attempt})")
        try:
            response = requests.get(url, headers=headers, params=querystring)
            response.raise_for_status()
            result = response.json()[0]
            return result['status'], result['email_provider'], ""
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")
            if attempt == 3:
                return "-", "-", f"Unable to verify email after 3 attempts: {e}"
    return "-","-","Internal Error"


