import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
import os

# --- Authentication Configuration ---
# The google-github-actions/auth action sets the GOOGLE_APPLICATION_CREDENTIALS
# environment variable, which gspread's default credential search can find.
try:
    gc = gspread.service_account()
    print("Authentication to Google Sheets successful.")
except Exception as e:
    print(f"Error authenticating with Google: {e}")
    # Exit if authentication fails, as nothing else can be done.
    exit(1)

# --- Region-to-Sheet Mapping ---
REGION_TO_SHEET_MAP = {
    "AHOADA": "Bayelsa", "ALPHA 1": "Alpha", "ALPHA 2": "Alpha",
    "BAYELSA": "Bayelsa", "BETA 1": "Beta", "BETA 2": "Beta",
    "CALABAR": "Calabar", "EKET REGION": "uyo", "GAMMA 1": "Gamma",
    "GAMMA 2": "Gamma", "IKOT EKPENE": "Calabar", "OGOJA": "Calabar",
    "UYO REGION": "Akwa_Ibom"
}

def process_single_region():
    """
    Main function triggered by the GitHub Action.
    Processes a SINGLE region passed via an environment variable.
    """
    try:
        # Get data from environment variables
        temp_sheet_id = os.environ['TEMP_SHEET_ID']
        main_sheet_id = os.environ['MAIN_SHEET_ID']
        # Get the single region for this job from the matrix
        region_to_process = os.environ['SELECTED_REGION']

        print(f"--- Starting process for region: {region_to_process} ---")

        # 1. Map the region to its target sheet name
        region_upper = region_to_process.upper()
        sheet_name = REGION_TO_SHEET_MAP.get(region_upper)
        if not sheet_name:
            raise ValueError(f"No sheet mapping found for region '{region_to_process}'")

        # 2. Read the uploaded data from the temporary Google Sheet
        temp_ss = gc.open_by_key(temp_sheet_id)
        temp_sheet = temp_ss.get_worksheet(0)
        uploaded_df = pd.DataFrame(temp_sheet.get_all_records())
        # Standardize column names
        uploaded_df.columns = [str(col).strip().upper() for col in uploaded_df.columns]
        
        # 3. Read the "Reference" sheet from the main spreadsheet
        dest_ss = gc.open_by_key(main_sheet_id)
        ref_sheet = dest_ss.worksheet("Reference")
        ref_df = pd.DataFrame(ref_sheet.get_all_records())
        ref_df.columns = [str(col).strip().upper() for col in ref_df.columns]
        ref_df['REGIONS'] = ref_df['REGIONS'].str.upper()
        
        # 4. Find the old rows to delete for this specific region
        target_sheet = dest_ss.worksheet(sheet_name)
        region_ref_info = ref_df[ref_df['REGIONS'] == region_upper]

        if not region_ref_info.empty:
            start_idx = int(region_ref_info.iloc[0].get('TARGET FIRST INDEX', 0))
            end_idx = int(region_ref_info.iloc[0].get('TARGET LAST INDEX', 0))
            
            if start_idx > 0 and end_idx >= start_idx:
                print(f"In sheet '{sheet_name}', deleting old rows for '{region_to_process}' from index {start_idx} to {end_idx}.")
                target_sheet.delete_rows(start_idx, end_idx)
            else:
                print(f"No valid row range found for '{region_to_process}' in Reference sheet. Skipping deletion.")
        else:
            print(f"Region '{region_to_process}' not found in Reference sheet. Skipping deletion.")

        # 5. Filter the uploaded data to get only the rows for the current region
        new_rows_df = uploaded_df[uploaded_df['REGIONS'].str.upper() == region_upper]

        if not new_rows_df.empty:
            print(f"Found {len(new_rows_df)} new rows to add to sheet '{sheet_name}'.")
            # 6. Append the new data to the end of the sheet
            # We find the new last row *after* potential deletions
            last_row = len(target_sheet.get_all_values())
            set_with_dataframe(target_sheet, new_rows_df, row=last_row + 1, include_index=False, include_column_header=False)
            print(f"Successfully added {len(new_rows_df)} record(s) for '{region_to_process}'.")
        else:
            print(f"No new data found for region '{region_to_process}' in the uploaded file.")

        print(f"--- Finished process for region: {region_to_process} ---")

    except Exception as e:
        print(f"An error occurred while processing region '{os.environ.get('SELECTED_REGION', 'N/A')}': {e}")
        # Re-raise the exception to make the GitHub Actions job fail
        raise e

if __name__ == "__main__":
    process_single_region()
