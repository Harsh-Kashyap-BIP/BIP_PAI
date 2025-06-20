import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import re


def cold_email_batcher_advanced(
        df: pd.DataFrame,
        company_col: str,
        priority_col: str,
        email_provider_col: str,
        job_title_col: str,
        department_col: str,
        employee_count_col: str,
        mailboxes: int,
        emails_per_mailbox: int,
        batch_duration_days: int,
        start_date: str = None
) -> pd.DataFrame:
    """
    Advanced cold email batching system with company size-based targeting rules.

    Logic:
    1. Segments companies by employee count (0-50, 51-100, 101-200, 201-500, 501-1000)
    2. Applies different targeting rules for each segment:
       - Small companies (0-50): Target C-level executives
       - Medium companies (51-200): Target VPs and Directors
       - Large companies (201-1000): Focus on department heads in sales/marketing/operations
    3. Selects leads based on priority scoring within each company
    4. Distributes selected leads across batches based on mailbox capacity
    """

    # Create a copy to avoid modifying original dataframe
    df = df.copy()

    # Set start date
    start = datetime.strptime(start_date, "%Y-%m-%d") if start_date else datetime.today()
    total_emails_per_day = mailboxes * emails_per_mailbox

    # Initialize columns
    df[priority_col] = pd.to_numeric(df[priority_col], errors="coerce").fillna(0)
    df[employee_count_col] = pd.to_numeric(df[employee_count_col], errors="coerce")
    df["Status"] = "ready"
    df["Batch number"] = None
    df["Send Date"] = None
    df["Batch Name"] = None

    # Handle email providers
    df[email_provider_col] = df[email_provider_col].astype(str).str.lower().fillna("unknown")
    df["Reason"] = ""
    df.loc[df[email_provider_col].isin(["no_provider", "unknown", "nan"]), "Status"] = "unbatchable"
    df.loc[df[email_provider_col].isin(["no_provider", "unknown", "nan"]), "Reason"] = "No valid email provider"

    # Company size-based targeting rules
    size_rules = [
        {
            "name": "Small Companies (0-50)",
            "min": 0, "max": 50, "limit": 4,
            "primary_roles": ["ceo", "founder", "co-founder", "owner", "president"],
            "secondary_roles": ["director", "head of", "vp", "vice president"],
            "exclusion_roles": ["intern", "assistant", "coordinator", "analyst"],
            "target_departments": None,  # Any department
            "exclusion_departments": None
        },
        {
            "name": "Small-Medium Companies (51-100)",
            "min": 51, "max": 100, "limit": 6,
            "primary_roles": ["ceo", "founder", "co-founder", "vp", "vice president"],
            "secondary_roles": ["director", "head of", "senior manager", "manager"],
            "exclusion_roles": ["intern", "assistant", "analyst", "coordinator"],
            "target_departments": None,
            "exclusion_departments": None
        },
        {
            "name": "Medium Companies (101-200)",
            "min": 101, "max": 200, "limit": 8,
            "primary_roles": ["director", "vp", "vice president", "head of"],
            "secondary_roles": ["senior manager", "manager", "senior director"],
            "exclusion_roles": ["ceo", "founder", "analyst", "coordinator"],
            "target_departments": ["sales", "marketing", "operations", "growth", "business development"],
            "exclusion_departments": ["hr", "human resources", "legal", "finance", "accounting"]
        },
        {
            "name": "Large Companies (201-500)",
            "min": 201, "max": 500, "limit": 10,
            "primary_roles": ["director", "head of", "senior director", "vp", "vice president"],
            "secondary_roles": ["senior manager", "manager"],
            "exclusion_roles": ["ceo", "president", "analyst", "coordinator"],
            "target_departments": ["sales", "marketing", "operations", "growth", "business development"],
            "exclusion_departments": ["hr", "human resources", "legal", "finance", "accounting"]
        },
        {
            "name": "Very Large Companies (501-1000)",
            "min": 501, "max": 1000, "limit": 13,
            "primary_roles": ["senior manager", "director", "head of", "senior director"],
            "secondary_roles": ["manager", "vp", "vice president"],
            "exclusion_roles": ["ceo", "president", "analyst"],
            "target_departments": ["sales", "marketing", "operations", "growth", "business development"],
            "exclusion_departments": ["hr", "human resources", "legal", "finance", "accounting"]
        }
    ]

    # Get company employee counts (use max in case of duplicates)
    company_emp = df.groupby(company_col)[employee_count_col].max().reset_index()

    def get_rule(emp_count):
        """Get the appropriate rule based on employee count"""
        if pd.isna(emp_count) or emp_count > 1000:
            return None
        for rule in size_rules:
            if rule["min"] <= emp_count <= rule["max"]:
                return rule
        return None

    # Apply rules to companies
    company_emp["rule"] = company_emp[employee_count_col].apply(get_rule)
    company_emp["limit"] = company_emp["rule"].apply(lambda r: r["limit"] if r else 0)

    # Merge rules back to main dataframe
    df = df.merge(company_emp[[company_col, "rule", "limit"]], on=company_col, how="left")

    # Mark unbatchable companies (no rule applies)
    df.loc[df["rule"].isna(), "Status"] = "unbatchable"
    df.loc[df["rule"].isna(), "Reason"] = "Company size not supported (>1000 employees or invalid employee count)"

    # Normalize text columns for matching
    df[job_title_col] = df[job_title_col].astype(str).str.lower().fillna("")
    df[department_col] = df[department_col].astype(str).str.lower().fillna("")

    # Filter to batchable leads
    batchable = df[df["Status"] == "ready"].copy()

    def matches_role(title, role_list):
        """Check if job title matches any role in the list"""
        if not role_list:
            return False
        title = str(title).lower()
        for role in role_list:
            if role.lower() in title:
                return True
        return False

    def matches_department(dept, dept_list):
        """Check if department matches any in the list"""
        if not dept_list:
            return True  # If no restriction, match all
        dept = str(dept).lower()
        for target_dept in dept_list:
            if target_dept.lower() in dept:
                return True
        return False

    def is_eligible(row):
        """Check if a lead is eligible based on the company's rules"""
        rule = row["rule"]
        if not rule:
            return False, "No targeting rule for company size"

        title = row[job_title_col]
        dept = row[department_col]

        # Check exclusion roles first
        if matches_role(title, rule["exclusion_roles"]):
            return False, f"Job title '{title}' is in exclusion roles"

        # Check department restrictions
        if rule["target_departments"] and not matches_department(dept, rule["target_departments"]):
            return False, f"Department '{dept}' not in target departments"

        if rule["exclusion_departments"] and matches_department(dept, rule["exclusion_departments"]):
            return False, f"Department '{dept}' is in exclusion departments"

        # Must match primary or secondary roles
        if matches_role(title, rule["primary_roles"]):
            return True, f"Matches primary role criteria (title: {title})"
        elif matches_role(title, rule["secondary_roles"]):
            return True, f"Matches secondary role criteria (title: {title})"

        return False, f"Job title '{title}' doesn't match target roles"

    # Filter eligible leads and track reasons
    eligible_leads = []
    ineligible_leads = []

    for idx, row in batchable.iterrows():
        is_eligible_result, reason = is_eligible(row)
        if is_eligible_result:
            eligible_leads.append(idx)
            df.loc[idx, "Reason"] = reason
        else:
            ineligible_leads.append(idx)
            df.loc[idx, "Status"] = "unbatchable"
            df.loc[idx, "Reason"] = reason

    batchable = batchable.loc[eligible_leads].copy()

    # Select leads for each company and track selection reasons
    selected_leads = {}  # Store by email provider
    company_selection_stats = {}

    for company, group in batchable.groupby(company_col):
        if group.empty:
            continue

        rule = group["rule"].iloc[0]
        limit = rule["limit"]

        # Prioritize primary roles over secondary roles
        primary = group[group.apply(lambda x: matches_role(x[job_title_col], rule["primary_roles"]), axis=1)]
        secondary = group[group.apply(lambda x: matches_role(x[job_title_col], rule["secondary_roles"]), axis=1)]

        # Combine with primary roles first
        combined = pd.concat([primary, secondary])

        # Remove duplicates based on email (assuming email is unique identifier)
        if "Email" in combined.columns:
            combined = combined.drop_duplicates(subset=["Email"])

        # Select top leads based on priority score
        all_sorted = combined.sort_values(by=priority_col, ascending=False)
        top_leads = all_sorted.head(limit)
        remaining_leads = all_sorted.iloc[limit:]

        # Track selection stats
        company_selection_stats[company] = {
            'selected': len(top_leads),
            'available': len(all_sorted),
            'limit': limit
        }

        # Update reasons for selected leads
        for idx in top_leads.index:
            provider = df.loc[idx, email_provider_col]
            if provider not in selected_leads:
                selected_leads[provider] = []
            selected_leads[provider].append(idx)

            rank = list(all_sorted.index).index(idx) + 1
            df.loc[
                idx, "Reason"] = f"Selected (rank {rank}/{len(all_sorted)} in company, priority: {df.loc[idx, priority_col]})"

        # Update reasons for leads that weren't selected due to company limit
        for idx in remaining_leads.index:
            df.loc[idx, "Status"] = "future"
            df.loc[idx, "Reason"] = f"Not selected - company limit reached ({limit} leads max for {rule['name']})"

    # Create provider-specific batches
    for provider, lead_indices in selected_leads.items():
        if not lead_indices:
            continue

        # Get leads for this provider and sort by priority
        provider_leads = df.loc[lead_indices].sort_values(by=priority_col, ascending=False)

        # Calculate batches for this provider
        total_batches = int(np.ceil(len(provider_leads) / total_emails_per_day))

        for i in range(total_batches):
            batch_start = i * total_emails_per_day
            batch_end = min((i + 1) * total_emails_per_day, len(provider_leads))

            batch_number = i + 1
            batch_date = start + timedelta(days=(i % batch_duration_days))

            # Get indices for this batch
            batch_indices = provider_leads.iloc[batch_start:batch_end].index

            # Update batch information
            df.loc[batch_indices, "Batch number"] = batch_number
            df.loc[batch_indices, "Send Date"] = batch_date.strftime("%Y-%m-%d")
            df.loc[batch_indices, "Batch Name"] = f"{provider} batch-{batch_number}"

            # Update reason to include batch info
            for idx in batch_indices:
                current_reason = df.loc[idx, "Reason"]
                df.loc[idx, "Reason"] = f"{current_reason} → Assigned to {provider} batch-{batch_number}"

    # Update status for leads that weren't selected and don't have reasons yet
    no_reason_mask = (df["Status"] == "ready") & (df["Reason"] == "")
    df.loc[no_reason_mask, "Status"] = "future"
    df.loc[no_reason_mask, "Reason"] = "Eligible but not selected in current batch cycle"

    # Clean up temporary columns
    df = df.drop(columns=["rule", "limit"], errors="ignore")

    return df.reset_index(drop=True)


def print_batch_summary(df: pd.DataFrame):
    """Print a summary of the batching results"""
    print("=" * 80)
    print("COLD EMAIL BATCHING SUMMARY")
    print("=" * 80)

    status_counts = df["Status"].value_counts()
    print(f"Total leads processed: {len(df)}")
    print(f"Leads batched: {len(df[df['Batch number'].notna()])}")
    print(f"Leads marked as future: {status_counts.get('future', 0)}")
    print(f"Unbatchable leads: {status_counts.get('unbatchable', 0)}")

    # Provider-specific batch summary
    if "Batch Name" in df.columns:
        batched_leads = df[df["Batch Name"].notna()]
        if not batched_leads.empty:
            print(f"\nBatch distribution by provider:")
            provider_batches = batched_leads["Batch Name"].value_counts().sort_index()
            for batch_name, count in provider_batches.items():
                print(f"  {batch_name}: {count} leads")

    # Reason summary
    if "Reason" in df.columns:
        print(f"\nTop reasons for status:")
        reason_counts = df["Reason"].value_counts().head(10)
        for reason, count in reason_counts.items():
            print(f"  {reason}: {count}")

    print("=" * 80)


# Example usage
# if __name__ == "__main__":
#     # Load your data
#     try:
#         data = pd.read_csv("../sheet.csv", encoding='ISO-8859-1')
#
#         result_df = cold_email_batcher_advanced(
#             df=data,
#             company_col="Company",
#             priority_col="Priority Score",
#             email_provider_col="Email Providers",
#             job_title_col="Title",
#             department_col="Departments",
#             employee_count_col="Employees",
#             mailboxes=5,
#             emails_per_mailbox=30,
#             batch_duration_days=10,
#             start_date=date.today().strftime("%Y-%m-%d")
#         )
#
#         # Save results
#         result_df.to_csv("batched_output.csv", index=False)
#
#         # Print summary
#         print_batch_summary(result_df)
#
#         # Show sample of results
#         print("\nSample of batched results:")
#         sample = result_df[result_df["Batch number"].notna()].head(10)
#         if not sample.empty:
#             print(sample[["Company", "Title", "Departments", "Priority Score", "Batch number", "Send Date",
#                           "Status"]].to_string())
#
#     except FileNotFoundError:
#         print("Error: sheet.csv not found. Please check the file path.")
#     except Exception as e:
#         print(f"Error processing data: {e}")