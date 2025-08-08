import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from flask import Flask, request, jsonify

# --- Authentication Configuration ---
# The script will look for your renamed key file.
SERVICE_ACCOUNT_FILE = 'service-account-key.json'

# Authenticate with Google using the service account key.
try:
    gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
    print("Authentication to Google Sheets successful.")
except Exception as e:
    print(f"Error authenticating with Google: {e}")
    gc = None

# --- UPDATED: Your specific region-to-sheet mapping is now included ---
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

app = Flask(__name__)

@app.route('/process-sheet', methods=['POST'])
def process_sheet_endpoint():
    """
    This endpoint receives data from Apps Script, reads an uploaded sheet,
    processes the data by deleting old records and appending new ones.
    """
    if not gc:
        return jsonify(status="error", message="Server authentication error: Could not connect to Google services."), 500
    
    try:
        data = request.get_json()
        temp_sheet_id = data['tempSheetId']
        main_sheet_id = data['mainSheetId']
        selected_regions = data['regions']

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
            sheet = dest_ss.worksheet(sheet_name)
            
            # 4a. Delete old rows for the selected regions, starting from the bottom to avoid shifting issues
            rows_to_delete = []
            for region in regions_uc:
                if region in ref_df.index:
                    ref_info = ref_df.loc[region]
                    start_idx = int(ref_info.get('TARGET FIRST INDEX', 0))
                    end_idx = int(ref_info.get('TARGET LAST INDEX', 0))
                    if start_idx > 0 and end_idx >= start_idx:
                        rows_to_delete.append({'start': start_idx, 'end': end_idx})
            
            # Sort ranges to delete from the bottom up
            for r in sorted(rows_to_delete, key=lambda x: x['start'], reverse=True):
                sheet.delete_rows(r['start'], r['end'])

            # 4b. Filter the uploaded data to get only the rows for the current regions
            new_rows_df = uploaded_df[uploaded_df['REGIONS'].str.upper().isin(regions_uc)]

            if not new_rows_df.empty:
                # 4c. Append the new data to the end of the sheet
                set_with_dataframe(sheet, new_rows_df, row=len(sheet.get_all_values()) + 1, include_index=False, include_column_header=False)
                total_added += len(new_rows_df)
        
        response_message = f"Python process complete. Added {total_added} record(s) across {len(sheet_to_regions)} sheet(s)."
        return jsonify(status="success", message=response_message), 200

    except Exception as e:
        print(f"Error during sheet processing: {e}")
        return jsonify(status="error", message=f"Python Error: {str(e)}"), 500