import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
import os
import json
import sys

# --- Authentication Configuration ---
try:
    creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if not creds_path:
        raise ValueError("Google credentials not found! Ensure the 'google-github-actions/auth' step ran successfully.")
    gc = gspread.service_account(filename=creds_path)
    print("Authentication to Google Sheets successful.")
except Exception as e:
    print(f"Error authenticating with Google: {e}")
    sys.exit(1) # Exit if authentication fails

# --- Region-to-Sheet Mapping ---
REGION_TO_SHEET_MAP = {
    "AHOADA": "Bayelsa", "ALPHA 1": "Alpha", "ALPHA 2": "Alpha",
    "BAYELSA": "Bayelsa", "BETA 1": "Beta", "BETA 2": "Beta",
    "CALABAR": "Calabar", "EKET REGION": "uyo", "GAMMA 1": "Gamma",
    "GAMMA 2": "Gamma", "IKOT EKPENE": "Calabar", "OGOJA": "Calabar",
    "UYO REGION": "Akwa_Ibom"
}

def process_sheets_in_batches():
    """
    Main function that processes regions in batches based on their destination sheet.
    If one batch fails, it logs the error and continues to the next.
    """
    failed_sheets = [] # Keep track of any batches that fail
    try:
        # Get data from environment variables
        temp_sheet_id = os.environ['TEMP_SHEET_ID']
        main_sheet_id = os.environ['MAIN_SHEET_ID']
        
        # Read the JSON string of regions and parse it into a Python list
        regions_json_str = os.environ['SELECTED_REGIONS_JSON']
        selected_regions = json.loads(regions_json_str)

        print(f"Processing started for regions: {selected_regions}")

        # 1. Read data from sheets (same as your original script)
        temp_ss = gc.open_by_key(temp_sheet_id)
        temp_sheet = temp_ss.get_worksheet(0)
        uploaded_df = pd.DataFrame(temp_sheet.get_all_records())
        uploaded_df.columns = [str(col).strip().upper() for col in uploaded_df.columns]
        
        dest_ss = gc.open_by_key(main_sheet_id)
        ref_sheet = dest_ss.worksheet("Reference")
        ref_df = pd.DataFrame(ref_sheet.get_all_records())
        ref_df.columns = [str(col).strip().upper() for col in ref_df.columns]
        ref_df['REGIONS'] = ref_df['REGIONS'].str.upper()
        ref_df.set_index('REGIONS', inplace=True)

        # 2. Group selected regions by their destination sheet name
        sheet_to_regions = {}
        for region in selected_regions:
            sheet_name = REGION_TO_SHEET_MAP.get(region.upper())
            if not sheet_name:
                print(f"Warning: No sheet mapping found for region '{region}'. Skipping.")
                continue
            if sheet_name not in sheet_to_regions:
                sheet_to_regions[sheet_name] = []
            sheet_to_regions[sheet_name].append(region.upper())

        # 3. Process each destination sheet (batch) one by one
        for sheet_name, regions_in_batch in sheet_to_regions.items():
            print(f"\n--- Processing Batch for Sheet: {sheet_name} ---")
            try:
                # This 'try' block ensures that a failure here won't stop the whole script
                sheet = dest_ss.worksheet(sheet_name)
                
                # Delete old rows for the regions in this batch
                rows_to_delete = []
                for region in regions_in_batch:
                    if region in ref_df.index:
                        ref_info = ref_df.loc[region]
                        start_idx = int(ref_info.get('TARGET FIRST INDEX', 0))
                        end_idx = int(ref_info.get('TARGET LAST INDEX', 0))
                        if start_idx > 0 and end_idx >= start_idx:
                            rows_to_delete.append({'start': start_idx, 'end': end_idx})
                
                for r in sorted(rows_to_delete, key=lambda x: x['start'], reverse=True):
                    print(f"Deleting rows {r['start']} to {r['end']} in sheet '{sheet_name}'")
                    sheet.delete_rows(r['start'], r['end'])

                # Append new data for the regions in this batch
                new_rows_df = uploaded_df[uploaded_df['REGIONS'].str.upper().isin(regions_in_batch)]

                if not new_rows_df.empty:
                    print(f"Found {len(new_rows_df)} new rows to add.")
                    last_row = len(sheet.get_all_values())
                    set_with_dataframe(sheet, new_rows_df, row=last_row + 1, include_index=False, include_column_header=False)
                    print(f"Successfully processed batch for sheet '{sheet_name}'.")

            except Exception as batch_error:
                # If an error occurs, log it and add the sheet to our failed list
                print(f"!!! ERROR processing batch for sheet '{sheet_name}': {batch_error}")
                failed_sheets.append(sheet_name)
                # The 'continue' is implicit as the loop will just go to the next item

    except Exception as overall_error:
        print(f"A critical error occurred: {overall_error}")
        sys.exit(1)

    # 4. Final check: if any batches failed, exit with an error code to make the job fail
    if failed_sheets:
        print(f"\nProcess complete, but the following sheet(s) failed: {', '.join(failed_sheets)}")
        sys.exit(1)
    else:
        print("\nPython process completed successfully for all batches.")


if __name__ == "__main__":
    process_sheets_in_batches()
