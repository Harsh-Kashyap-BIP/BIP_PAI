import random
from datetime import date
from http.client import HTTPException

import numpy as np
import asyncio

from crud.projects import get_project_by_id
from upload_file_superbase import upload_df_to_supabase_async
from utility.ai_generated_ice_breakers import generate_ice_breakers_chain
from utility.batching import cold_email_batcher_advanced
from utility.company_linkedIn_data import get_company_linkedin_data
from utility.email_verifier import lead_email_verifier
from utility.exa_webite_summary import get_website_summary
from utility.priority_score import get_priority_score


# class googleSheetRequestModel(BaseModel):
#     project_id: str = Field(description="Project ID")
#     original_sheet_url: str = Field(description="Google Sheet URL")
#     proceed_on_invalid_email: bool = Field(False, description="Proceed with invalid email")
#     openai_key: str = Field(
#         default="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",
#         description="OpenAI API key")
#     ss_masters_key: str = Field(default="4ded372450msh25fbcd11ee504f3p106f2cjsn678047a92674",
#                                 description="SSMASTERS API key")
#     exa_api_key: str = Field(default="0d8c86b4-8bee-44ff-b77b-d4befdb1f9e2", description="Exa AI API key")

async def generate_personalized_sheet(data, request, column_names):
    '''

    :param data:pandas dataframe
    :param request: googleSheetRequestModel
    :param column_names: the names of the columns
    :return: pandas dataframe
    '''

    # Initializing the column name variables
    FIRST_NAME = column_names['first_name']
    LAST_NAME = column_names['last_name']
    COMPANY_NAME = column_names['company_name']
    EMAIL = column_names['email']
    JOB_TITLE = column_names['job_title']
    SENIORITY=column_names['seniority']
    if SENIORITY is None:
        data['Seniority'] = ''
        SENIORITY ='Seniority'
    INDUSTRY = column_names['industry']
    DEPARTMENT = column_names['department']
    if DEPARTMENT is None:
        data['Department'] = ''
        DEPARTMENT = 'Department'
    COMPANY_WEBSITE = column_names['company_website']
    COMPANY_LINKEDIN = column_names['company_linkedin']
    EMPLOYEE_COUNT = column_names['employee_count']

    EMPLOYEE_COUNT_LINKEDIN = 'Number of employees (LinkedIn)'

    #Getting the project details
    from fastapi import HTTPException

    print("Getting project details")
    try:
        project_details = await get_project_by_id(request.project_id)
    except Exception as e:
        print(f"Error fetching project details: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve project details")

    num_emails = len(data[EMAIL])
    num_website = len(data[COMPANY_WEBSITE])
    num_linkedin_companies = len(data[COMPANY_LINKEDIN])

    # Additional columns
    error_log = np.empty(num_emails, dtype=object)
    priority_reason = np.empty(num_emails, dtype=object)
    priority_score = np.empty(num_emails, dtype=object)
    number_of_employees_from_linkedin = np.empty(num_linkedin_companies, dtype=object)
    linkedin_company_data = np.empty(num_linkedin_companies, dtype=object)
    ice_breaker_selection_reason = np.empty(num_website, dtype=object)
    ice_breaker_selected = np.empty(num_website, dtype=object)
    ice_breaker_options = np.empty(num_website, dtype=object)
    exa_website_summary = np.empty(num_website, dtype=object)
    email_providers = np.empty(num_emails, dtype=object)
    is_email_valid = np.empty(num_emails, dtype=object)

    # Retain api call data
    company_website_search_history = {}
    company_linkedin_search_history = {}
    company_number_of_employees_search_history = {}
    company_common_name = {}

    request_limit_per_minute = 5
    wait_time = 20  # seconds

    for i, (index, row) in enumerate(data.iterrows()):
        print(f'----ROW:{i + 1}----')

        # Error Log configuration
        if error_log[i] is None:
            error_log[i] = ""

        # Email Verification and Email Providers
        verification_status, email_provider, verification_error = await lead_email_verifier(
            email=row[EMAIL],
            api_key=request.ss_masters_key
        )
        is_email_valid[i] = verification_status
        email_providers[i] = email_provider
        if len(verification_error):
            error_log[i] += f"*{verification_error} \n"

        if (request.proceed_on_invalid_email and is_email_valid[i] != 'valid') or is_email_valid[i] == 'valid':

            # Creating a loop for parallel api calls
            loop = asyncio.get_event_loop()
            f1 = f2 = None

            # Exa Website Summary
            if row[COMPANY_WEBSITE] in company_website_search_history.keys():
                exa_website_summary[i] = company_website_search_history[row[COMPANY_WEBSITE]]
            else:
                f1 = loop.run_in_executor(None, get_website_summary, row[COMPANY_WEBSITE], request.exa_api_key)

            # Company LinkedIn data
            if row[COMPANY_LINKEDIN] in company_linkedin_search_history.keys():
                linkedin_company_data[i] = company_linkedin_search_history[row[COMPANY_LINKEDIN]]
                number_of_employees_from_linkedin[i] = company_number_of_employees_search_history[row[COMPANY_LINKEDIN]]
            else:
                f2 = loop.run_in_executor(None, get_company_linkedin_data, row[COMPANY_LINKEDIN],
                                          request.ss_masters_key)

            # Getting the api results
            futures = []
            labels = []
            if f1:
                futures.append(f1)
                labels.append("website")
            if f2:
                futures.append(f2)
                labels.append("linkedin")

            if futures:
                results = await asyncio.gather(*futures)

                for label, result in zip(labels, results):
                    # Company Website Data
                    if label == "website":
                        error = ''
                        exa_website_summary[i], error = result
                        company_website_search_history[row[COMPANY_WEBSITE]] = exa_website_summary[i]
                        if len(error):
                            error_log[i] += f"* {error} \n"

                    # Company LinkedIn data
                    if label == "linkedin":
                        error = ''
                        linkedin_company_data[i], number_of_employees_from_linkedin[i], error = result
                        company_linkedin_search_history[row[COMPANY_LINKEDIN]] = linkedin_company_data[i]
                        company_number_of_employees_search_history[row[COMPANY_LINKEDIN]] = \
                        number_of_employees_from_linkedin[i]
                        if len(error):
                            error_log[i] += f"* {error} \n"

            # Ice breakers
            error = ''
            ice_breaker_options[i], ice_breaker_selected[i], ice_breaker_selection_reason[
                i], error = await generate_ice_breakers_chain(website_summary=exa_website_summary[i],
                                                              linkedin_summary=linkedin_company_data[i],
                                                              openai_api_key=request.openai_key)
            # Priority score
            error = ''
            priority_level, error = await get_priority_score(job_title=row[JOB_TITLE],
                                                             seniority=row[SENIORITY],
                                                             industry=row[INDUSTRY],
                                                             desc=project_details['description'],
                                                             department=row[DEPARTMENT],
                                                             company_size=row[EMPLOYEE_COUNT],
                                                             openai_api_key=request.openai_key)
            priority_score[i] = priority_level['priority_score']
            priority_reason[i] = priority_level['reason']
            if len(error):
                error_log[i] += f"* {error} \n"

        # Rate limiting: wait after processing each batch of 2 emails,
        # but skip waiting after the last batch
        if (i + 1) % request_limit_per_minute == 0 and (i + 1) != num_emails:
            await asyncio.sleep(wait_time)

    # Creating new columns
    data['Email Valid'] = is_email_valid.tolist()
    data['Email Providers'] = email_providers.tolist()
    data['Exa Website Summary'] = exa_website_summary.tolist()
    data['Company LinkedIn data '] = linkedin_company_data.tolist()
    data['Number of employees (LinkedIn)'] = number_of_employees_from_linkedin.tolist()
    data['Ice Breakers Options'] = ice_breaker_options.tolist()
    data['Ice Breaker Selected'] = ice_breaker_selected.tolist()
    data['Ice Breaker Selection Reason'] = ice_breaker_selection_reason.tolist()
    data['Priority Score'] = priority_score.tolist()
    data['Priority Score Reason'] = priority_reason.tolist()
    data['Error Log'] = error_log.tolist()
    data.fillna('-', inplace=True)

    # await upload_df_to_supabase_async(df=data, file_prefix='big_sheet')

    # Batching based on the mailbox
    result_df = cold_email_batcher_advanced(
        df=data,
        company_col=COMPANY_NAME,
        priority_col="Priority Score",
        email_provider_col="Email Providers",
        job_title_col=JOB_TITLE,
        department_col=DEPARTMENT,
        employee_count_col=EMPLOYEE_COUNT,
        mailboxes=project_details['no_of_mailbox'],
        emails_per_mailbox=project_details['emails_per_mailbox'],
        batch_duration_days=project_details['batch_duration_days'],
        start_date=date.today().strftime("%Y-%m-%d"),
    )

    return result_df

