# app.py
# TL Sheet Updater and Auditor Streamlit Application

import streamlit as st
import gspread
from google.oauth2.service_account import Credentials as ServiceAccountCredentials # Though using OAuth now
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow # For OAuth
import os.path # For checking credentials file

from datetime import datetime, timedelta
import pytz
import pandas as pd
import requests
import io
import zipfile
import traceback
import re

# --- Configuration (Part 2 - Adapted for Streamlit) ---
# Ensure these IDs and names are correct
SOURCE_SPREADSHEET_ID = '1Fo3-zzub663AnMLIPpgHVpP5Dsb7Zk2H554PFDHliY8'
DEST_SPREADSHEET_ID = '17SFltoaYiEVVHDN7flctrHn1TKj01xCCyrsoiCN7L8c'
SOURCE_SHEET_INDEX = 0 # 'Form responses 1'
# DEST_TARGET_MONTH_SHEET_NAME will be set by user input

EXCEL_SHAREPOINT_URL = 'https://panaceasolutionsllc-my.sharepoint.com/:x:/p/saqib/EQuk1cHbsH9KtafkGnzmaiABUBtt4x-5vamjr5hdpGsQwg'

# Column indices (0-based for reading lists of lists from gspread)
# Source Sheet (SS1 - 'Form responses 1')
SRC_COL_DATE_OF_SERVICE = 2
SRC_COL_COVERAGE_TYPE = 3
SRC_COL_PROVIDER_NAME_READ = 4 # For validation
SRC_COL_SCHEDULED_ZOOM = 6
SRC_COL_UPLOADED_EOD = 7
SRC_COL_SCRIBE_NAME = 13

# Destination Sheet (SS2 - 'May' tab, etc.)
DEST_COL_A_SCRIBE_NAME_READ = 0
DEST_COL_B_LEAD_READ = 1
DEST_COL_C_DATE_READ = 2
DEST_COL_E_TASK_ASSIGNED_READ = 4
DEST_COL_F_PROVIDER_COVERED_READ = 5
DEST_COL_Q_SCHEDULED_READ = 16
DEST_COL_R_UPLOADED_READ = 17

# Column numbers (1-based for gspread cell updates)
DEST_COL_Q_SCHEDULED_WRITE = 17
DEST_COL_R_UPLOADED_WRITE = 18

SPECIAL_PROVIDER_FULL_NAMES = [
    "Alison Blake", "Amanda DeBois", "Heather Reynolds",
    "Melanie Arrington", "Nikki Kelly", "Sarah Driggs", "Danelle Schmutz"
]

# !!! USER ACTION REQUIRED: Verify/Update this list precisely !!!
# Providers managed by "Saqib Sherwani" - names as they appear in SS2 Column F
MY_MANAGED_PROVIDERS = [
    "Erin Henderson",
    "Kei Batangan",
    "Dr. Christine Potterjones", # Corrected from "Dr. Christine Potterjones" if that was a typo
    "Amanda Reda Goglio",
    "Dr. Kirmani Moe",
    "Dr. Mark Basham",
    "Alison Blake",       # Also in SPECIAL_PROVIDER_FULL_NAMES
    "Amanda DeBois",      # Also in SPECIAL_PROVIDER_FULL_NAMES
    "Heather Reynolds",   # Also in SPECIAL_PROVIDER_FULL_NAMES
    "Melanie Arrington",  # Also in SPECIAL_PROVIDER_FULL_NAMES
    "Nikki Kelly",        # Also in SPECIAL_PROVIDER_FULL_NAMES
    "Sarah Driggs",       # Also in SPECIAL_PROVIDER_FULL_NAMES
    "Danelle Schmutz",    # Also in SPECIAL_PROVIDER_FULL_NAMES
    "Celeste Callinan",
    "Seana Wishart",
    "Beth Sanford",
    "Dr. Kaleb Wartgow",
    "Chinor Fattahi"
    # Add any other specific providers from SS2 Col F that are managed by Saqib Sherwani
    # and remove any that are not. The list you provided has been used here.
    # Ensure these names are exactly as they appear in SS2, Column F.
]


# Path to your OAuth credentials.json file
CREDENTIALS_FILE = 'credentials.json' # Download from Google Cloud Console
TOKEN_FILE = 'token.json' # Will be created after first successful auth
OAUTH_SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


# --- Helper Functions (Part 3 - Adapted for Streamlit) ---

# @st.cache_data # Cache simple parsing functions # Removed cache for now from normalize, not a heavy func
def normalize_provider_name(name_str):
    if not isinstance(name_str, str): return ""
    name = str(name_str).strip()
    name = re.sub(r'^[Dd][Rr][sS]?\.\s*', '', name)
    suffixes_patterns = [
        r',\s*NP-C', r',\s*FNP-C', r',\s*PA-C', r',\s*NP', r',\s*PA',
        r',\s*FNP', r',\s*Ng', r',\s*APRN', r',\s*MD', r',\s*DO', r',\s*DNP',
        r'\s+NP-C$', r'\s+FNP-C$', r'\s+PA-C$', r'\s+NP$', r'\s+PA$',
        r'\s+FNP$', r'\s+Ng$', r'\s+APRN$', r'\s+MD$', r'\s+DO$', r'\s+DNP$'
    ]
    for suffix_pattern in suffixes_patterns:
        name = re.sub(suffix_pattern, '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*\([^)]*\)$', '', name)
    name = name.replace(',', '')
    return name.strip().lower()

# @st.cache_data # Removed cache, parse_date_flexible is called many times with different inputs
def parse_date_flexible(date_str):
    if not date_str or not isinstance(date_str, str): return None
    date_str = date_str.strip()
    formats_to_try = ["%d/%m/%Y", "%m/%d/%Y", "%d/%m/%y", "%m/%d/%y", "%Y-%m-%d"]
    for fmt in formats_to_try:
        try: return datetime.strptime(date_str, fmt).date()
        except ValueError: continue
    try: return datetime.fromisoformat(date_str.split(' ')[0]).date()
    except ValueError: pass
    return None

@st.cache_resource # Cache the gspread client resource
def authenticate_gspread_service_account():
    """
    Authenticates with Google Sheets using a Service Account from Streamlit Secrets.
    """
    try:
        # Check if the secrets for the service account are available
        if "gcp_service_account" in st.secrets:
            creds_dict = st.secrets["gcp_service_account"]
        else:
            st.error("GCP Service Account credentials not found in Streamlit Secrets.")
            st.info("Please add them to your Streamlit Cloud app's secret management.")
            return None

        # Authorize the client
        creds = ServiceAccountCredentials.from_service_account_info(creds_dict, scopes=OAUTH_SCOPES)
        client = gspread.authorize(creds)
        st.success("Successfully authenticated with Google Sheets using Service Account!")
        return client

    except Exception as e:
        st.error(f"Failed to authorize gspread client with Service Account: {e}")
        traceback.print_exc()
        return None


@st.cache_data(ttl=3600)
def load_and_map_excel_data(_excel_url, _special_provider_full_names_list, _date_parser_func, _cutoff_date):
    # st.info(f"Downloading Excel data (up to {_cutoff_date.strftime('%Y-%m-%d')})...")
    if "?" in _excel_url: download_url = _excel_url + "&download=1"
    else: download_url = _excel_url + "?download=1"

    excel_lookup_dict = {}; dynamic_excel_col_for_provider = {}
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(download_url, headers=headers, timeout=45, allow_redirects=True)
        response.raise_for_status()
        # st.info("Excel: Initial HTTP request successful.")
        if not response.content.startswith(b'PK\x03\x04'):
            st.error("Excel Error: Downloaded content is not a valid Excel file (PK header missing)."); return None, None
        
        excel_content_io = io.BytesIO(response.content)
        df_excel = pd.read_excel(excel_content_io, sheet_name="Count", engine='openpyxl', header=1)
        # st.info(f"Excel: Successfully read 'Count' tab. Columns: {df_excel.columns.tolist()}")

        date_column_name = None
        for col in df_excel.columns:
            if str(col).strip().lower() == 'date': date_column_name = col; break
        if not date_column_name:
            if len(df_excel.columns) > 1 and 'unnamed: 0' in str(df_excel.columns[0]).lower():
                potential_date_col = df_excel.columns[1]
                if str(potential_date_col).strip().lower() == 'date': date_column_name = potential_date_col
                elif df_excel[potential_date_col].astype(str).str.contains(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',regex=True).any():
                    date_column_name = potential_date_col; st.info(f"Excel: Guessed Date column: {date_column_name}")
            elif 'Date' in df_excel.columns: date_column_name = 'Date'
        if not date_column_name: st.error("Excel Error: 'Date' column not identified."); return None, None
        # st.info(f"Excel: Using '{date_column_name}' as Date column.")

        def convert_excel_date(val):
            if pd.isna(val): return None
            if isinstance(val, datetime): return val.date()
            parsed_dt = _date_parser_func(str(val).strip())
            if parsed_dt: return parsed_dt
            excel_formats = ["%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]
            for fmt in excel_formats:
                try: return datetime.strptime(str(val).strip().split(" ")[0], fmt.split(" ")[0]).date()
                except ValueError: continue
            return None
        df_excel['ParsedDate'] = df_excel[date_column_name].apply(convert_excel_date)

        for full_name in _special_provider_full_names_list:
            first_name = full_name.split(' ')[0]
            for col_hdr in df_excel.columns:
                if str(col_hdr).strip().startswith(first_name + "/"):
                    dynamic_excel_col_for_provider[full_name] = str(col_hdr).strip(); break
            if full_name not in dynamic_excel_col_for_provider:
                st.warning(f"Excel: Column not found for special provider: {full_name}")
        # st.info(f"Excel: Dynamic provider mapping: {len(dynamic_excel_col_for_provider)} mapped.")
        
        for ss2_name, excel_col in dynamic_excel_col_for_provider.items():
            if excel_col in df_excel.columns:
                for _, row in df_excel.iterrows():
                    p_date = row['ParsedDate']
                    if p_date and p_date <= _cutoff_date:
                        excel_lookup_dict[(p_date, ss2_name)] = row[excel_col]
        # st.info(f"Excel lookup dict: {len(excel_lookup_dict)} entries (up to {_cutoff_date.strftime('%Y-%m-%d')}).")
        if len(excel_lookup_dict) == 0 and df_excel['ParsedDate'].notna().any():
             st.warning("Excel Warning: excel_lookup_dict empty but parseable dates exist. Check provider column names or date range.")
        return excel_lookup_dict, dynamic_excel_col_for_provider
    except Exception as e: st.error(f"Error loading/processing Excel: {e}"); return None, None

@st.cache_data(ttl=3600)
def build_ss1_validation_map(_source_data_list, _date_parser_func, _cutoff_date):
    ss1_map = {}
    if not _source_data_list or len(_source_data_list) <= 1: return ss1_map
    processed_count = 0
    for idx, row in enumerate(_source_data_list[1:]):
        try:
            req_idx = [SRC_COL_DATE_OF_SERVICE, SRC_COL_SCRIBE_NAME, SRC_COL_COVERAGE_TYPE, SRC_COL_PROVIDER_NAME_READ]
            if len(row) <= max(req_idx): continue
            p_date = _date_parser_func(str(row[SRC_COL_DATE_OF_SERVICE]))
            if not p_date or p_date > _cutoff_date: continue
            scribe_norm = str(row[SRC_COL_SCRIBE_NAME]).strip().lower() # Corrected: remove extra ']'
            if scribe_norm:
                key = (p_date, scribe_norm)
                if key not in ss1_map: ss1_map[key] = []
                ss1_map[key].append({
                    "coverage_type": str(row[SRC_COL_COVERAGE_TYPE]).strip(), # Corrected: remove extra ']'
                    "provider_name_ss1": str(row[SRC_COL_PROVIDER_NAME_READ]).strip(), # Corrected: remove extra ']'
                    "ss1_row_num": idx + 2
                })
                processed_count +=1
        except Exception: pass
    return ss1_map

# run_comprehensive_validation_checks (from previous response, with syntax corrections)
def run_comprehensive_validation_checks(dest_data_list, ss1_reports_map, excel_data_lookup_dict,
                                        special_provider_full_names, my_managed_providers_list,
                                        date_parser_func, cutoff_date):
    validation_errors = []
    if not dest_data_list or len(dest_data_list) <= 1:
        validation_errors.append("Validation Aborted: Destination data (SS2) is empty."); return validation_errors
    
    # st.info(f"Running Validation on SS2 (Lead: Saqib Sherwani, up to {cutoff_date.strftime('%Y-%m-%d')})...")
    ss2_active_coverage_map = {}
    processed_for_validation = 0

    for idx, ss2_row_data in enumerate(dest_data_list[1:]):
        ss2_row_num_1_based = idx + 2
        try:
            min_cols_ss2 = max(DEST_COL_A_SCRIBE_NAME_READ, DEST_COL_B_LEAD_READ, DEST_COL_C_DATE_READ,
                               DEST_COL_E_TASK_ASSIGNED_READ, DEST_COL_F_PROVIDER_COVERED_READ)
            if len(ss2_row_data) <= min_cols_ss2: continue
            if str(ss2_row_data[DEST_COL_B_LEAD_READ]).strip() != "Saqib Sherwani": continue
            
            parsed_date_ss2 = date_parser_func(str(ss2_row_data[DEST_COL_C_DATE_READ]))
            if not parsed_date_ss2 or parsed_date_ss2 > cutoff_date: continue

            provider_ss2_raw = str(ss2_row_data[DEST_COL_F_PROVIDER_COVERED_READ]).strip() # Corrected
            if provider_ss2_raw not in my_managed_providers_list: continue
            
            processed_for_validation += 1
            scribe_name_ss2_raw = str(ss2_row_data[DEST_COL_A_SCRIBE_NAME_READ]).strip() # Corrected
            task_ss2_raw = str(ss2_row_data[DEST_COL_E_TASK_ASSIGNED_READ]).strip()      # Corrected
            task_ss2_norm = task_ss2_raw.lower()
            norm_provider_ss2_f = normalize_provider_name(provider_ss2_raw)

            if task_ss2_norm in ["primary coverage", "backup coverage"]:
                ckey = (parsed_date_ss2, norm_provider_ss2_f)
                if ckey not in ss2_active_coverage_map: ss2_active_coverage_map[ckey] = []
                ss2_active_coverage_map[ckey].append({
                    "scribe": scribe_name_ss2_raw, "task": task_ss2_raw,
                    "ss2_row_num": ss2_row_num_1_based, "provider_as_in_col_f": provider_ss2_raw
                })

            is_special = provider_ss2_raw in special_provider_full_names
            if is_special:
                excel_key = (parsed_date_ss2, provider_ss2_raw)
                excel_val = "No Excel Entry"; excel_num_count = 0
                if excel_data_lookup_dict and excel_key in excel_data_lookup_dict:
                    excel_val = excel_data_lookup_dict[excel_key]
                    try:
                        if not pd.isna(excel_val) and str(excel_val).strip().upper() != "NW":
                            excel_num_count = int(float(str(excel_val)))
                    except ValueError: excel_num_count = 0
                if excel_num_count > 0:
                    if task_ss2_norm not in ["primary coverage", "backup coverage"]:
                        validation_errors.append(f"VALIDATION (RMS): '{provider_ss2_raw}' on {parsed_date_ss2.strftime('%Y-%m-%d')} (SS2 Row {ss2_row_num_1_based}), Excel count '{excel_val}', but SS2 Task '{task_ss2_raw}' not Primary/Backup.")
                elif task_ss2_norm in ["primary coverage", "backup coverage"]:
                     validation_errors.append(f"VALIDATION (RMS): SS2 (Row {ss2_row_num_1_based}) assigns Task '{task_ss2_raw}' to '{provider_ss2_raw}' on {parsed_date_ss2.strftime('%Y-%m-%d')}, but Excel shows no positive count (found: '{excel_val}').")
            else: # Non-Special
                scribe_name_ss2_norm = scribe_name_ss2_raw.lower() # Already done, but good for clarity
                if not scribe_name_ss2_norm and task_ss2_norm in ["primary coverage", "backup coverage"]:
                    validation_errors.append(f"VALIDATION (Non-RMS): SS2 Row {ss2_row_num_1_based}, Task '{task_ss2_raw}', Scribe Name missing for Provider '{provider_ss2_raw}'."); continue
                ss1_key = (parsed_date_ss2, scribe_name_ss2_norm)
                if ss1_key in ss1_reports_map:
                    ss1_entries = [e for e in ss1_reports_map.get(ss1_key, []) if e["coverage_type"].lower().strip() in ["primary coverage", "backup coverage"]] # Corrected list comprehension
                    if not ss1_entries and task_ss2_norm in ["primary coverage", "backup coverage"]:
                        all_tasks = [e["coverage_type"] for e in ss1_reports_map.get(ss1_key, [])] # Corrected list comprehension
                        details = f"(SS1 tasks: {', '.join(all_tasks)})" if all_tasks else "(SS1 no tasks)"
                        validation_errors.append(f"VALIDATION (Non-RMS): SS2 (Row {ss2_row_num_1_based}: Scribe '{scribe_name_ss2_raw}', Provider '{provider_ss2_raw}') Task '{task_ss2_raw}' on {parsed_date_ss2.strftime('%Y-%m-%d')}, but no Primary/Backup in SS1. {details}")
                    elif len(ss1_entries) > 1:
                        validation_errors.append(f"VALIDATION (Non-RMS): Scribe '{scribe_name_ss2_raw}' on {parsed_date_ss2.strftime('%Y-%m-%d')} has multiple ({len(ss1_entries)}) Primary/Backup in SS1. SS2 (Row {ss2_row_num_1_based}) for '{provider_ss2_raw}' ambiguous.")
                    elif len(ss1_entries) == 1:
                        ss1_cov = ss1_entries[0]; norm_ss1_prov = normalize_provider_name(ss1_cov["provider_name_ss1"])
                        if task_ss2_norm in ["primary coverage", "backup coverage"]:
                            if norm_provider_ss2_f != norm_ss1_prov: # Comparing normalized names
                                validation_errors.append(f"VALIDATION (Non-RMS): Scribe '{scribe_name_ss2_raw}' on {parsed_date_ss2.strftime('%Y-%m-%d')} (SS2 Row {ss2_row_num_1_based}), SS2 Provider '{provider_ss2_raw}', but SS1 (Row {ss1_cov['ss1_row_num']}) reports for '{ss1_cov['provider_name_ss1']}'.")
                        else:
                            validation_errors.append(f"VALIDATION (Non-RMS): SS1 (Row {ss1_cov['ss1_row_num']}) reports Scribe '{scribe_name_ss2_raw}' did '{ss1_cov['coverage_type']}' for '{ss1_cov['provider_name_ss1']}' on {parsed_date_ss2.strftime('%Y-%m-%d')}, but SS2 Task (Row {ss2_row_num_1_based}) is '{task_ss2_raw}'.")
                elif task_ss2_norm in ["primary coverage", "backup coverage"]:
                    validation_errors.append(f"VALIDATION (Non-RMS): SS2 (Row {ss2_row_num_1_based}: Scribe '{scribe_name_ss2_raw}', Provider '{provider_ss2_raw}') Task '{task_ss2_raw}' on {parsed_date_ss2.strftime('%Y-%m-%d')}, but NO entries in SS1 for Scribe/Date.")
        except Exception: pass # Silently skip problematic rows in validation pass to avoid stopping the whole validation

    for (date_k, norm_prov_k), assigns in ss2_active_coverage_map.items():
        if not assigns: continue
        prov_col_f_check = assigns[0]["provider_as_in_col_f"]
        if prov_col_f_check in my_managed_providers_list and len(assigns) > 1:
            details = "; ".join([f"Scribe '{a['scribe']}' Task='{a['task']}' (SS2 Row {a['ss2_row_num']})" for a in assigns])
            validation_errors.append(f"VALIDATION (Uniqueness): Provider '{prov_col_f_check}' on {date_k.strftime('%Y-%m-%d')} has {len(assigns)} active coverages in SS2: {details}")
    return validation_errors


# --- Streamlit App UI and Logic (Part 4) ---
st.set_page_config(page_title="TL Sheet Updater and Auditor", layout="wide")
st.title("💻 TL Sheet Updater and Auditor")

# --- Authentication ---
if 'gc' not in st.session_state or st.session_state.gc is None:
    with st.spinner("Authenticating with Google Sheets..."):
        st.session_state.gc = authenticate_gspread_service_account()

# If authentication fails, stop the app
if st.session_state.gc is None:
    st.stop()

gc = st.session_state.gc

# --- Date Filtering ---
ist_timezone = pytz.timezone('Asia/Kolkata')
now_ist = datetime.now(ist_timezone)
yesterday_ist_date = (now_ist - timedelta(days=1)).date()
st.sidebar.markdown(f"**Processing data up to (IST): {yesterday_ist_date.strftime('%Y-%m-%d')}**")

# --- Month Selection ---
months = ["May", "June", "July", "August", "September", "October", "November", "December"] 
current_month_name = now_ist.strftime("%B")
default_month_index = months.index(current_month_name) if current_month_name in months else months.index("May") # Default to May if current month not in list
selected_month = st.sidebar.selectbox("Select Month to Process:", months, index=default_month_index)
# DEST_TARGET_MONTH_SHEET_NAME is dynamically set by selected_month for this session

# --- Data Loading ---
# Using a general key for caching to force re-evaluation if month or gc changes
# However, gc object itself might not be directly hashable for st.cache_data if its internal state changes
# A simpler approach for Streamlit is to load data within button actions or manage via session_state for complex objects

# Function to load data (not cached here, will be called inside button actions to ensure freshness if needed)
# Placed with other helper functions

@st.cache_data(ttl=900) # Cache for 15 minutes
def load_all_data(_gc_client_ref, _source_id, _dest_id, _src_idx, _dest_month_name, 
                  _excel_url, _special_providers, _date_parser, _cutoff_date):
    # This function helps group data loading for caching.
    # It calls get_sheet_data, which now needs the corrected gspread_client.
    active_gc = st.session_state.get('gc', None)
    if not active_gc:
        st.warning("Gspread client not authenticated for load_all_data. Cannot load data.")
        return None, None, None, None

    data_ss1, _ = get_sheet_data(active_gc, _source_id, _src_idx, "Source SS1")
    data_ss2, ws_ss2 = get_sheet_data(active_gc, _dest_id, _dest_month_name, "Destination SS2")
    data_excel_lookup, _ = load_and_map_excel_data(_excel_url, _special_providers, _date_parser, _cutoff_date)
    return data_ss1, data_ss2, ws_ss2, data_excel_lookup

# Corrected get_sheet_data function
def get_sheet_data(gspread_client, spreadsheet_id, sheet_name_or_index, sheet_type="Destination"):
    try:
        # Explicitly use open_by_key as it's known to work in your environment
        if hasattr(gspread_client, 'open_by_key'):
            spreadsheet = gspread_client.open_by_key(spreadsheet_id)
        elif hasattr(gspread_client, 'open_by_id'): # Fallback, though likely to fail for you
            st.warning(f"Attempting to use 'open_by_id' for {sheet_type} as 'open_by_key' was not found (this is unexpected).")
            spreadsheet = gspread_client.open_by_id(spreadsheet_id)
        else:
            st.error(f"CRITICAL: gspread client for {sheet_type} has neither 'open_by_key' nor 'open_by_id'.")
            return None, None

        # Proceed to get worksheet
        if isinstance(sheet_name_or_index, int):
            worksheet = spreadsheet.get_worksheet(sheet_name_or_index)
        else:
            worksheet = spreadsheet.worksheet(sheet_name_or_index)
        
        st.info(f"Successfully accessed the sheet: '{worksheet.title}' in '{spreadsheet.title}'")
        return worksheet.get_all_values(), worksheet
        
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"Error: Tab '{sheet_name_or_index}' not found in {sheet_type} Spreadsheet (ID: {spreadsheet_id}).")
    except AttributeError as ae: # Catch the specific error if it still occurs
        st.error(f"AttributeError when trying to open {sheet_type} sheet (ID: {spreadsheet_id}, Tab: {sheet_name_or_index}): {ae}")
        st.error("This indicates an issue with the gspread client object's methods. Please ensure gspread is correctly installed and authenticated.")
        traceback.print_exc() # For more detailed error in logs
    except Exception as e:
        st.error(f"An unexpected error occurred reading {sheet_type} sheet (ID: {spreadsheet_id}, Tab: {sheet_name_or_index}): {e}")
        traceback.print_exc()
    return None, None

# --- Main Application Logic ---
st.header(f"Actions for Month: {selected_month}")

# Load destination sheet first to check if tab exists
dest_data, dest_worksheet = get_sheet_data(gc, DEST_SPREADSHEET_ID, selected_month, "Destination SS2")

if dest_data and dest_worksheet:
    # Load other data only if destination sheet is accessible
    source_data, _ = get_sheet_data(gc, SOURCE_SPREADSHEET_ID, SOURCE_SHEET_INDEX, "Source SS1")
    excel_data_lookup, _ = load_and_map_excel_data(EXCEL_SHAREPOINT_URL, SPECIAL_PROVIDER_FULL_NAMES, parse_date_flexible, yesterday_ist_date)

    dest_lookup_map = {}
    if len(dest_data) > 1:
        for i, row in enumerate(dest_data[1:]):
            try:
                if len(row) > max(DEST_COL_A_SCRIBE_NAME_READ, DEST_COL_C_DATE_READ):
                    p_date = parse_date_flexible(str(row[DEST_COL_C_DATE_READ]))
                    if p_date and p_date <= yesterday_ist_date:
                        s_norm = str(row[DEST_COL_A_SCRIBE_NAME_READ]).strip().lower()
                        if s_norm: dest_lookup_map[(s_norm, p_date)] = i + 2
            except: pass 
    # st.info(f"Built SS2 lookup map with {len(dest_lookup_map)} entries for Scribe/Date matching (up to {yesterday_ist_date.strftime('%Y-%m-%d')}).")


    # Button for Updating Patient Counts
    if st.button(f"Update TL Sheet with Patient Counts for '{selected_month}'"):
        with st.spinner(f"Processing updates for {selected_month}... This may take a moment."):
            updates_to_make = []
            processed_details_update = [] # Renamed to avoid conflict if validation uses same name
            excel_val_mismatches_upd = []
            ss1_r_mismatches_upd = []
            excel_processed_s2_rows_upd = set()

            # Phase 1: Excel Updates
            if excel_data_lookup is not None and len(dest_data) > 1:
                for s2_idx, s2_row in enumerate(dest_data[1:]):
                    s2_num = s2_idx + 2
                    min_cols = max(DEST_COL_F_PROVIDER_COVERED_READ, DEST_COL_C_DATE_READ, DEST_COL_E_TASK_ASSIGNED_READ, DEST_COL_Q_SCHEDULED_READ, DEST_COL_R_UPLOADED_READ)
                    if len(s2_row) <= min_cols: continue
                    p_date_s2 = parse_date_flexible(str(s2_row[DEST_COL_C_DATE_READ]))
                    if not p_date_s2 or p_date_s2 > yesterday_ist_date: continue
                    prov_s2 = str(s2_row[DEST_COL_F_PROVIDER_COVERED_READ]).strip()

                    if prov_s2 in SPECIAL_PROVIDER_FULL_NAMES:
                        excel_processed_s2_rows_upd.add(s2_num)
                        excel_key = (p_date_s2, prov_s2)
                        if excel_key in excel_data_lookup:
                            excel_raw_c = excel_data_lookup[excel_key]; excel_num_c = 0
                            try:
                                if not pd.isna(excel_raw_c) and str(excel_raw_c).strip().upper() != "NW":
                                    excel_num_c = int(float(str(excel_raw_c)))
                            except ValueError: excel_num_c = 0
                            
                            if excel_num_c > 0:
                                task_raw = str(s2_row[DEST_COL_E_TASK_ASSIGNED_READ]).strip()
                                if task_raw.lower() in ["primary coverage", "backup coverage"]:
                                    ex_q = str(s2_row[DEST_COL_Q_SCHEDULED_READ]).strip()
                                    ex_r = str(s2_row[DEST_COL_R_UPLOADED_READ]).strip()
                                    q_u = False; r_u = False; q_m = f"Q Exists('{ex_q}')"; r_m = f"R Exists('{ex_r}')"
                                    if not ex_q: updates_to_make.append(gspread.Cell(s2_num, DEST_COL_Q_SCHEDULED_WRITE, excel_num_c)); q_u=True; q_m=f"Q to {excel_num_c}"
                                    if not ex_r: updates_to_make.append(gspread.Cell(s2_num, DEST_COL_R_UPLOADED_WRITE, excel_num_c)); r_u=True; r_m=f"R to {excel_num_c}"
                                    if q_u or r_u: processed_details_update.append({"s":f"Excel-{prov_s2}","d":p_date_s2.strftime('%Y-%m-%d'),"dr":s2_num,"qn":q_m,"rs":r_m,"qa":q_u,"ra":r_u})
                                else: excel_val_mismatches_upd.append(f"UPDATE EXCEL VAL: SS2R {s2_num} ('{prov_s2}', {p_date_s2}). ExcelCnt={excel_num_c}, SS2Task='{task_raw}'. No upd.")
            # st.info(f"Revitalize Roster {len(excel_processed_s2_rows_upd)} rows for RMS providers.")

            # Phase 2: SS1 Updates
            if source_data and len(source_data) > 1:
                for i, src_row in enumerate(source_data[1:]):
                    try:
                        if len(src_row) <= max(SRC_COL_COVERAGE_TYPE, SRC_COL_DATE_OF_SERVICE, SRC_COL_SCRIBE_NAME, SRC_COL_SCHEDULED_ZOOM, SRC_COL_UPLOADED_EOD): continue
                        p_src_date = parse_date_flexible(str(src_row[SRC_COL_DATE_OF_SERVICE]))
                        if not p_src_date or p_src_date > yesterday_ist_date: continue
                        if str(src_row[SRC_COL_COVERAGE_TYPE]).strip().lower() not in ["primary coverage", "backup coverage"]: continue
                        s_src_norm = str(src_row[SRC_COL_SCRIBE_NAME]).strip().lower()
                        if not s_src_norm: continue
                        
                        match_key = (s_src_norm, p_src_date)
                        if match_key in dest_lookup_map:
                            dest_r_num = dest_lookup_map[match_key]
                            if dest_r_num in excel_processed_s2_rows_upd: continue

                            q_u_ss1=False; r_u_ss1=False; q_s_ss1="No Q change"; r_s_ss1="No R change"
                            dest_idx = dest_r_num - 1
                            
                            ss2_target_row = dest_data[dest_idx] if 0 <= dest_idx < len(dest_data) else []
                            
                            ex_q = str(ss2_target_row[DEST_COL_Q_SCHEDULED_READ]).strip() if len(ss2_target_row) > DEST_COL_Q_SCHEDULED_READ else ""
                            s_val_q = str(src_row[SRC_COL_SCHEDULED_ZOOM]).strip()
                            if not ex_q and s_val_q: updates_to_make.append(gspread.Cell(dest_r_num, DEST_COL_Q_SCHEDULED_WRITE,s_val_q)); q_u_ss1=True; q_s_ss1=f"SS1:Q to '{s_val_q}'"
                            elif ex_q: q_s_ss1=f"SS1:Q Exists('{ex_q}')"
                            
                            ex_r = str(ss2_target_row[DEST_COL_R_UPLOADED_READ]).strip() if len(ss2_target_row) > DEST_COL_R_UPLOADED_READ else ""
                            s_val_r = str(src_row[SRC_COL_UPLOADED_EOD]).strip()
                            if not ex_r and s_val_r: updates_to_make.append(gspread.Cell(dest_r_num,DEST_COL_R_UPLOADED_WRITE,s_val_r)); r_u_ss1=True; r_s_ss1=f"SS1:R to '{s_val_r}'"
                            elif ex_r: 
                                r_s_ss1=f"SS1:R Exists('{ex_r}')"
                                if s_val_r and ex_r != s_val_r: ss1_r_mismatches_upd.append(f"SS1_R_INFO: Scribe {str(src_row[SRC_COL_SCRIBE_NAME])}, {p_src_date},DRow {dest_r_num}. DestR:'{ex_r}',SrcR:'{s_val_r}'. No upd.")
                            
                            if q_u_ss1 or r_u_ss1: processed_details_update.append({"s":str(src_row[SRC_COL_SCRIBE_NAME]),"d":p_src_date.strftime('%Y-%m-%d'),"dr":dest_r_num,"qn":q_s_ss1,"rs":r_s_ss1,"qa":q_u_ss1,"ra":r_u_ss1})
                    except Exception: pass 
            # st.info("Update EOD Report processing complete for other providers.")

            if updates_to_make:
                st.info(f"Attempting to apply {len(updates_to_make)} cell updates to '{dest_worksheet.title}'...")
                try:
                    dest_worksheet.update_cells(updates_to_make, value_input_option='USER_ENTERED')
                    st.success(f"Successfully applied {len(updates_to_make)} cell updates!")
                except Exception as e:
                    st.error(f"Error applying updates to Google Sheet: {e}")
            else:
                st.info("No updates to apply to the TL Sheet based on current data.")

            # Display Summary of Updates
            st.subheader("Update Process Summary:")
            updated_rows_count = len(set(cell.row for cell in updates_to_make))
            st.write(f"Number of unique destination rows with cell changes applied: {updated_rows_count}")
            
            actionable_details = [d for d in processed_details_update if d["qa"] or d["ra"]]
            if actionable_details:
                st.write("Details of updates made (up to 20 shown directly):")
                display_lines = []
                for detail in sorted(actionable_details, key=lambda x: (x["dr"], x["d"])):
                    source_type = "Excel" if detail["s"].startswith("N/A (Excel") else "SS1"
                    display_lines.append(f"- DestRow: {detail['dr']}, Date: {detail['d']}, Src: {source_type}, Scribe: {detail['s']}, Q: {detail['qn']}, R: {detail['rs']}")
                
                if len(display_lines) > 20:
                    for line in display_lines[:20]: st.text(line)
                    with st.expander(f"Show all {len(display_lines)} update details"):
                        for line in display_lines: st.text(line)
                else:
                    for line in display_lines: st.text(line)
            else: st.write("No values in TL Sheet were changed in this run.")

            if excel_val_mismatches_upd:
                st.warning("Excel Validation Mismatches During Update Phase (No update made for these):")
                with st.expander("Show Excel Validation Mismatches"):
                    for msg in excel_val_mismatches_upd: st.text(f"  - {msg}")
            if ss1_r_mismatches_upd:
                st.info("SS1 Column R Information (R not updated due to existing value or mismatch):")
                with st.expander("Show SS1 R Mismatches/Info"):
                    for msg in ss1_r_mismatches_upd: st.text(f"  - {msg}")
        st.balloons()


    if st.button(f"Verfify Entries in '{selected_month}' TL Report"):
        with st.spinner(f"Running comprehensive validation for {selected_month}..."):
            ss1_val_map = {}
            if source_data:
                ss1_val_map = build_ss1_validation_map(source_data, parse_date_flexible, yesterday_ist_date)
            
            # Use the excel_data_lookup that was already loaded for the update phase if available
            excel_lookup_for_val = excel_data_lookup if 'excel_data_lookup' in locals() and excel_data_lookup is not None else None


            validation_issues = run_comprehensive_validation_checks(
                dest_data, ss1_val_map, excel_lookup_for_val,
                SPECIAL_PROVIDER_FULL_NAMES, MY_MANAGED_PROVIDERS,
                parse_date_flexible, yesterday_ist_date
            )

            st.subheader(f"Data Validation Report (Lead: Saqib Sherwani, up to {yesterday_ist_date.strftime('%Y-%m-%d')})")
            if validation_issues:
                st.warning(f"Found {len(validation_issues)} issues in TL Reports:")
                # Display logic for validation_issues (e.g., first 20 then expander)
                if len(validation_issues) > 20:
                    st.text("First 20 validation issues:")
                    for i, error_msg in enumerate(validation_issues[:20]): st.text(f"  {i+1}. {error_msg}")
                    with st.expander(f"Show all {len(validation_issues)} validation issues"):
                        for i, error_msg in enumerate(validation_issues): st.text(f"  {i+1}. {error_msg}")
                else:
                    for i, error_msg in enumerate(validation_issues): st.text(f"  {i+1}. {error_msg}")
            else:
                st.success("No discrepancies found in SS2 for providers managed by Saqib Sherwani based on the defined validation rules.")
        st.balloons()
else:
    st.error(f"Could not load data for sheet '{selected_month}' in Destination (SS2). Please ensure the tab exists and try again, or select a different month.")

st.sidebar.markdown("---")
st.sidebar.info("This app was created by Saqib Sherwani for his own use - automating patient count entries in TL Sheet.")
