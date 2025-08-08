import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
import os

# --- Authentication Configuration ---
# This script uses the authentication provided by the GitHub workflow.
# It does NOT read the key file directly.
try:
    gc = gspread.service_account() # The google-github-actions step handles auth
    print("Authentication to Google Sheets successful.")
except Exception as e:
    print(f"Error authenticating with Google: {e}")
    gc = None

# --- Your specific region-to-sheet mapping ---
REGION_TO_SHEET_MAP = {
    "AHOADA": "Bayelsa",
    "ALPHA 1": "Alpha",
    "ALPHA 2": "Alpha",
    "BAYELSA": "Bayelsa",
    "BETA 1": "Beta",
    "BETA 2": "Beta",
    "CALABAR": "Calabar",
    "EKET REGION": "uyo",
    "GAMMA 1": "Gamma",
    "GAMMA 2": "Gamma",
    "IKOT EKPENE": "Calabar",
    "OGOJA": "Calabar",
    "UYO REGION": "Akwa_Ibom"
}


def process_sheets():
    """
    This is the main function that runs when the GitHub Action is triggered.
    """
    if not gc:
        raise Exception("Authentication failed. Cannot connect to Google services.")
    
    try:
        # Get data from environment variables set by the GitHub workflow
        temp_sheet_id = os.environ['TEMP_SHEET_ID']
        main_sheet_id = os.environ['MAIN_SHEET_ID']
        
        regions_input = os.environ['SELECTED_REGIONS']
        selected_regions = [r.strip() for r in regions_input.split(',')]

        print(f"Processing started for regions: {selected_regions}")

        # 1. Read the uploaded data from the temporary Google Sheet
        temp_ss = gc.open_by_key(temp_sheet_id)
        temp_sheet = temp_ss.get_worksheet(0)
        uploaded_df = pd.DataFrame(temp_sheet.get_all_records())
        uploaded_df.columns = [col.strip().upper() for col in uploaded_df.columns]
        
        # 2. Read the "Reference" sheet from your main spreadsheet
        dest_ss = gc.open_by_key(main_sheet_id)
        ref_sheet = dest_ss.worksheet("Reference")
        ref_df = pd.DataFrame(ref_sheet.get_all_records())
        ref_df.columns = [col.strip().upper() for col in ref_df.columns]
        ref_df['REGIONS'] = ref_df['REGIONS'].str.upper()
        ref_df.set_index('REGIONS', inplace=True)

        # 3. Group the selected regions by their destination sheet name
        sheet_to_regions = {}
        for region in selected_regions:
            sheet_name = REGION_TO_SHEET_MAP.get(region.upper())
            if not sheet_name:
                raise ValueError(f"No sheet mapping found for region '{region}'")
            if sheet_name not in sheet_to_regions:
                sheet_to_regions[sheet_name] = []
            sheet_to_regions[sheet_name].append(region.upper())

        total_added = 0

        # 4. Process each destination sheet one by one
        for sheet_name, regions_uc in sheet_to_regions.items():
            print(f"Processing sheet: {sheet_name} for regions: {regions_uc}")
            sheet = dest_ss.worksheet(sheet_name)
            
            # 4a. Delete old rows for the selected regions
            rows_to_delete = []
            for region in regions_uc:
                if region in ref_df.index:
                    ref_info = ref_df.loc[region]
                    start_idx = int(ref_info.get('TARGET FIRST INDEX', 0))
                    end_idx = int(ref_info.get('TARGET LAST INDEX', 0))
                    if start_idx > 0 and end_idx >= start_idx:
                        rows_to_delete.append({'start': start_idx, 'end': end_idx})
            
            # Sort ranges to delete from the bottom up to avoid shifting issues
            for r in sorted(rows_to_delete, key=lambda x: x['start'], reverse=True):
                print(f"Deleting rows {r['start']} to {r['end']} in sheet '{sheet_name}'")
                sheet.delete_rows(r['start'], r['end'])

            # 4b. Filter the uploaded data to get only the rows for the current regions
            new_rows_df = uploaded_df[uploaded_df['REGIONS'].str.upper().isin(regions_uc)]

            if not new_rows_df.empty:
                print(f"Found {len(new_rows_df)} new rows to add to sheet '{sheet_name}'")
                # 4c. Append the new data to the end of the sheet
                set_with_dataframe(sheet, new_rows_df, row=len(sheet.get_all_values()) + 1, include_index=False, include_column_header=False)
                total_added += len(new_rows_df)
        
        print(f"Python process complete. Added {total_added} record(s) across {len(sheet_to_regions)} sheet(s).")

    except Exception as e:
        print(f"An error occurred during sheet processing: {e}")
        raise e

# This line makes the script run when called from the command line in the workflow
if __name__ == "__main__":
    process_sheets()
