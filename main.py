import os
import time
from datetime import datetime
from dotenv import load_dotenv
from vitro_cad_api import VitroCADAPIClient
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread
from typing import Optional, Dict, Any, List

load_dotenv()

# Google Sheets & Drive configuration
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

class VitroAutomation:
    """Main automation class for Vitro-CAD MP metadata migration."""
    
    def __init__(self, spreadsheet_id: str):
        """
        Initialize VitroAutomation.
        
        Args:
            spreadsheet_id: Google Sheets ID
        """
        self.spreadsheet_id = spreadsheet_id
        self.worksheet = None
        self.api_client = VitroCADAPIClient()
        self.api_client.get_token()
        
        # Initialize Google Sheets
        self._init_google_sheets()
        
        # Load environment constants
        self.load_constants()
        
        # Open Google Sheets
        self.spreadsheet = self.gc.open_by_key(self.spreadsheet_id)
        
        # Cache for lookups (to avoid duplicate requests)
        self.ctype_cache = {}  # MP_CTYPE_NAME -> MP_CTYPE_ID
        self.attr_cache = {}   # MP_ATTRIBUTE_NAME -> MP_ATTRIBUTE_ID
        self.list_cache = {}   # MP_LIST_NAME -> MP_LIST_ID
        
        # Batch update queue: {sheet_name: {col_idx: [(row, value), ...]}}
        self.batch_updates = {}
        
        # Rate limiting for Google Sheets API (60 req/min = 1 req/sec limit)
        self.google_api_delay = 1.0  # 1 second between batch updates to stay under 60 req/min
    
    def _init_google_sheets(self):
        """Initialize Google Sheets and Drive API clients."""
        try:
            creds = service_account.Credentials.from_service_account_file(
                os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
                scopes=SCOPES
            )
            self.gc = gspread.authorize(creds)
            self.sheets_service = build('sheets', 'v4', credentials=creds)
            self.drive_service = build('drive', 'v3', credentials=creds)
        except Exception as e:
            print(f"ERROR: Failed to initialize Google Sheets: {e}")
            raise
    
    def load_constants(self):
        """Load all constants from .env file."""
        self.CREATION_TOOL = "CREATION_TOOL"
        
        # Stage 1 Constants (Lists)
        self.LISTS_CTYPE_ID = os.getenv('LISTS_CTYPE_ID')
        
        # Stage 2 Constants (Content Types)
        self.CTYPES_LIST_ID = os.getenv('CTYPES_LIST_ID')
        self.CTYPES_CTYPE_ID = os.getenv('CTYPES_CTYPE_ID')
        self.CTYPES_DEFAULT_ELEMENT_ID = os.getenv('CTYPES_DEFAULT_ELEMENT_ID')
        
        # Stage 3 Constants (Attributes)
        self.ATTRIBUTES_LIST_ID = os.getenv('ATTRIBUTES_LIST_ID')
        self.ATTRIBUTES_CTYPE_ID = os.getenv('ATTRIBUTES_CTYPE_ID')
        
        # Stage 4 Constants (Add attributes to types)
        self.CONTENT_TYPE_FIELD_LIST_ID = os.getenv('CONTENT_TYPE_FIELD_LIST_ID')
        self.CONTENT_TYPE_FIELD_CTYPE_ID = os.getenv('CONTENT_TYPE_FIELD_CTYPE_ID')
        
        # Stage 5 Constants (Bind types to lists)
        self.LIST_CONTENT_TYPE_LIST_ID = os.getenv('LIST_CONTENT_TYPE_LIST_ID')
        self.LIST_CONTENT_TYPE_CTYPE_ID = os.getenv('LIST_CONTENT_TYPE_CTYPE_ID')
    
    def preload_caches(self):
        """Pre-load caches from existing sheet data to support non-sequential runs."""
        print("\nPre-loading caches from sheet data...")
        
        # Pre-load sheet headers
        self.sheet_headers = {}
        for sheet_name in ["LISTS", "CTYPES_UNIQUE", "ATTRIBUTES_UNIQUE", "ATTRIBUTES", "CTYPES"]:
            ws = self.get_sheet(sheet_name)
            if ws:
                self.sheet_headers[sheet_name] = ws.row_values(1)
        
        # Load lists
        list_records = self.get_all_records("LISTS")
        for record in list_records:
            list_name = record.get("MP_LIST_NAME")
            list_id = record.get("MP_LIST_ID")
            if list_name and list_id:
                self.list_cache[list_name] = list_id
        
        # Load content types
        ctype_records = self.get_all_records("CTYPES_UNIQUE")
        for record in ctype_records:
            ctype_name = record.get("MP_CTYPE_NAME")
            ctype_id = record.get("MP_CTYPE_ID")
            if ctype_name and ctype_id:
                self.ctype_cache[ctype_name] = ctype_id
        
        # Load attributes
        attr_records = self.get_all_records("ATTRIBUTES_UNIQUE")
        for record in attr_records:
            attr_name = record.get("MP_ATTRIBUTE_NAME")
            attr_id = record.get("MP_ATTRIBUTE_ID")
            if attr_name and attr_id:
                self.attr_cache[attr_name] = attr_id
        
        print(f"Loaded {len(self.list_cache)} lists, {len(self.ctype_cache)} types, {len(self.attr_cache)} attributes")
    
    def get_sheet(self, sheet_name: str):
        """Get worksheet by name."""
        try:
            return self.spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet '{sheet_name}' not found")
            return None
    
    def get_all_records(self, sheet_name: str) -> List[Dict]:
        """Get all records from a worksheet."""
        ws = self.get_sheet(sheet_name)
        if not ws:
            return []
        return ws.get_all_records()
    
    def convert_value(self, value: Any, target_type: str) -> Any:
        """Convert value to target type safely."""
        if value is None or value == "":
            return None
        
        if target_type.lower() == "bool" or target_type.lower() == "boolean":
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ('true', 'yes', '1', 'y')
            return bool(value)
        
        elif target_type.lower() == "int" or target_type.lower() == "integer":
            try:
                return int(float(str(value)))
            except (ValueError, TypeError):
                return None
        
        elif target_type.lower() == "float" or target_type.lower() == "decimal":
            try:
                return float(str(value))
            except (ValueError, TypeError):
                return None
        
        return value
    
    def convert_to_array(self, value: Any, delimiter: str = ",") -> List[str]:
        """Convert comma-separated string to array of strings."""
        if value is None or value == "":
            return None
        
        if isinstance(value, list):
            return value
        
        if isinstance(value, str):
            # Split by delimiter and strip whitespace
            return [item.strip() for item in value.split(delimiter) if item.strip()]
        
        return None
    
    def convert_to_iso8601(self, value: Any) -> Optional[str]:
        """Convert datetime value to ISO 8601 format in UTC. Example: 2024-01-28T08:40:26.168Z"""
        if value is None or value == "":
            return None
        
        # If already a string in ISO format, return as-is
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
            # Check if already in ISO format with Z
            if value.endswith('Z') or 'T' in value:
                return value
        
        # Try to parse various date formats
        try:
            from datetime import datetime
            
            # Try different date formats
            formats = [
                '%Y-%m-%dT%H:%M:%S.%fZ',  # 2024-01-28T08:40:26.168Z
                '%Y-%m-%dT%H:%M:%SZ',      # 2024-01-28T08:40:26Z
                '%Y-%m-%d %H:%M:%S.%f',    # 2024-01-28 08:40:26.168
                '%Y-%m-%d %H:%M:%S',       # 2024-01-28 08:40:26
                '%Y-%m-%d',                # 2024-01-28
                '%d.%m.%Y',                # 28.01.2024
                '%d/%m/%Y',                # 28/01/2024
            ]
            
            dt = None
            if isinstance(value, str):
                for fmt in formats:
                    try:
                        dt = datetime.strptime(value, fmt)
                        break
                    except ValueError:
                        continue
            
            if dt:
                # Convert to ISO 8601 UTC format
                return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        
        except Exception as e:
            print(f"Warning: Failed to convert date '{value}': {e}")
        
        return None
    
    def is_idempotent_record(self, record: Dict, id_column: str) -> bool:
        """Check if record already has ID (idempotency check) - uses record data."""
        cell_value = record.get(id_column)
        return cell_value is not None and cell_value != ""
    
    def queue_log_message(self, sheet_name: str, row_index: int, message: str, column_name: str = "SYNC_LOG"):
        """Queue a log message for batch update."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}"
        
        # Get headers
        headers = self.sheet_headers.get(sheet_name)
        if headers and column_name in headers:
            col_index = headers.index(column_name) + 1
            
            # Initialize batch entry if needed
            if sheet_name not in self.batch_updates:
                self.batch_updates[sheet_name] = {}
            if col_index not in self.batch_updates[sheet_name]:
                self.batch_updates[sheet_name][col_index] = []
            
            # Queue the update
            self.batch_updates[sheet_name][col_index].append((row_index, log_message))
        
        print(f"[{sheet_name}] [{timestamp}] {message}")
    
    def queue_cell_update(self, sheet_name: str, row_index: int, column_name: str, value: str):
        """Queue a cell update for batch update."""
        headers = self.sheet_headers.get(sheet_name)
        if headers and column_name in headers:
            col_index = headers.index(column_name) + 1
            
            # Initialize batch entry if needed
            if sheet_name not in self.batch_updates:
                self.batch_updates[sheet_name] = {}
            if col_index not in self.batch_updates[sheet_name]:
                self.batch_updates[sheet_name][col_index] = []
            
            # Queue the update
            self.batch_updates[sheet_name][col_index].append((row_index, value))
    
    def flush_batch_updates(self):
        """Send all queued updates to Google Sheets in batch."""
        if not self.batch_updates:
            return
        
        print(f"\nFlushing {sum(len(cols) for cols in self.batch_updates.values())} batch updates...")
        
        for sheet_name, col_updates in self.batch_updates.items():
            # Build list of updates in correct format for Google Sheets API
            data = []
            for col_index, cells in col_updates.items():
                for row_index, value in cells:
                    # Convert column index to letter (1-based: A, B, C, ... Z, AA, AB, ...)
                    col_letter = self._col_index_to_letter(col_index)
                    data.append({
                        'range': f'{sheet_name}!{col_letter}{row_index}',
                        'values': [[value]]
                    })
            
            # Send batch update using Google Sheets API
            if data:
                try:
                    self.sheets_service.spreadsheets().values().batchUpdate(
                        spreadsheetId=self.spreadsheet_id,
                        body={
                            'data': data,
                            'valueInputOption': 'RAW'
                        }
                    ).execute()
                    print(f"  ✓ {sheet_name}: {len(data)} cells updated")
                except Exception as e:
                    print(f"  ✗ {sheet_name}: Batch update failed: {e}")
        
        # Clear the queue
        self.batch_updates = {}
    
    def _col_index_to_letter(self, col_index: int) -> str:
        """Convert column index (1-based) to letter (A, B, ... Z, AA, AB, ...)"""
        result = ""
        while col_index > 0:
            col_index -= 1
            result = chr(65 + col_index % 26) + result
            col_index //= 26
        return result
    
    def update_sheet_cell(self, sheet_name: str, row_index: int, column_name: str, value: str):
        """Queue a cell update for batch processing."""
        self.queue_cell_update(sheet_name, row_index, column_name, value)
    
    def log_to_sheet(self, sheet_name: str, row_index: int, message: str, column_name: str = "SYNC_LOG"):
        """Queue a log message for batch processing."""
        self.queue_log_message(sheet_name, row_index, message, column_name)
    
    # ==================== STAGE 1: CREATE LISTS ====================
    def stage_1_create_lists(self):
        """Stage 1: Create lists from LISTS sheet."""
        print("\n" + "="*60)
        print("STAGE 1: Creating Lists")
        print("="*60)
        
        time.sleep(self.google_api_delay)  # Rate limiting
        records = self.get_all_records("LISTS")
        if not records:
            print("No records found in LISTS sheet")
            return
        
        for idx, record in enumerate(records, start=2):  # Start at row 2 (after header)
            try:
                # MIGRATION_APPROVED check
                if not self.convert_value(record.get("MIGRATION_APPROVED"), "bool"):
                    print(f"Row {idx}: Not approved for migration, skipping...")
                    continue
                
                # Idempotency check
                if self.is_idempotent_record(record, "MP_LIST_ID"):
                    print(f"Row {idx}: List already has ID, skipping...")
                    continue
                
                # Build payload
                data = {
                    "list_id": record.get("MP_LIST_OF_LISTS_ID"),
                    "content_type_id": self.LISTS_CTYPE_ID,
                    "name": record.get("MP_LIST_NAME"),
                    "table_name": record.get("MP_LIST_INTERNAL_NAME"),
                    "hidden": self.convert_value(record.get("MP_LIST_IS_HIDDEN"), "bool"),
                    "document_archive": self.convert_value(record.get("MP_LIST_IS_STORAGE"), "bool"),
                    "description": self.CREATION_TOOL
                }
                
                # Remove None values
                data = {k: v for k, v in data.items() if v is not None}
                
                # API call
                response = self.api_client.update_mp_list(data)
                
                if response and response.get('id'):
                    list_id = response.get('id')
                    self.update_sheet_cell("LISTS", idx, "MP_LIST_ID", list_id)
                    self.list_cache[record.get("MP_LIST_NAME")] = list_id
                    self.log_to_sheet("LISTS", idx, f"List created: {list_id}")
                else:
                    self.log_to_sheet("LISTS", idx, f"ERROR: {response}")
            
            except Exception as e:
                self.log_to_sheet("LISTS", idx, f"ERROR: {str(e)}")
        
        # Flush all queued batch updates
        time.sleep(self.google_api_delay)  # Rate limiting before flush
        self.flush_batch_updates()
    
    # ==================== STAGE 2: CREATE CONTENT TYPES ====================
    def stage_2_create_ctypes(self):
        """Stage 2: Create content types from CTYPES_UNIQUE sheet."""
        print("\n" + "="*60)
        print("STAGE 2: Creating Content Types")
        print("="*60)
        
        time.sleep(self.google_api_delay)  # Rate limiting
        records = self.get_all_records("CTYPES_UNIQUE")
        if not records:
            print("No records found in CTYPES_UNIQUE sheet")
            return
        
        for idx, record in enumerate(records, start=2):
            try:
                # MIGRATION_APPROVED check
                if not self.convert_value(record.get("MIGRATION_APPROVED"), "bool"):
                    print(f"Row {idx}: Not approved for migration, skipping...")
                    continue
                
                # Idempotency check
                if self.is_idempotent_record(record, "MP_CTYPE_ID"):
                    print(f"Row {idx}: Content type already has ID, skipping...")
                    continue
                
                # Build payload
                data = {
                    "list_id": self.CTYPES_LIST_ID,
                    "parent_id": self.CTYPES_DEFAULT_ELEMENT_ID,
                    "content_type_id": self.CTYPES_CTYPE_ID,
                    "name": record.get("MP_CTYPE_NAME"),
                    "folder": self.convert_value(record.get("MP_CTYPE_IS_FOLDER"), "bool"),
                    "description": self.CREATION_TOOL
                }
                
                data = {k: v for k, v in data.items() if v is not None}
                
                # API call
                response = self.api_client.update_mp_list(data)
                
                if response and response.get('id'):
                    ctype_id = response.get('id')
                    self.update_sheet_cell("CTYPES_UNIQUE", idx, "MP_CTYPE_ID", ctype_id)
                    self.ctype_cache[record.get("MP_CTYPE_NAME")] = ctype_id
                    self.log_to_sheet("CTYPES_UNIQUE", idx, f"Content type created: {ctype_id}")
                else:
                    self.log_to_sheet("CTYPES_UNIQUE", idx, f"ERROR: {response}")
            
            except Exception as e:
                self.log_to_sheet("CTYPES_UNIQUE", idx, f"ERROR: {str(e)}")
        
        # Flush all queued batch updates
        time.sleep(self.google_api_delay)  # Rate limiting before flush
        self.flush_batch_updates()
    
    # ==================== STAGE 3: CREATE ATTRIBUTES ====================
    def stage_3_create_attributes(self):
        """Stage 3: Create attributes using factory pattern based on MP_ATTRIBUTE_FIELD_TYPE."""
        print("\n" + "="*60)
        print("STAGE 3: Creating Attributes")
        print("="*60)
        
        time.sleep(self.google_api_delay)  # Rate limiting
        records = self.get_all_records("ATTRIBUTES_UNIQUE")
        if not records:
            print("No records found in ATTRIBUTES_UNIQUE sheet")
            return
        
        for idx, record in enumerate(records, start=2):
            try:
                # MIGRATION_APPROVED check
                if not self.convert_value(record.get("MIGRATION_APPROVED"), "bool"):
                    print(f"Row {idx}: Not approved for migration, skipping...")
                    continue
                
                # Idempotency check
                if self.is_idempotent_record(record, "MP_ATTRIBUTE_ID"):
                    print(f"Row {idx}: Attribute already has ID, skipping...")
                    continue
                
                # Base payload
                data = {
                    "list_id": self.ATTRIBUTES_LIST_ID,
                    "content_type_id": record.get("MP_ATTRIBUTE_FIELD_CTYPE_ID"),
                    "name": record.get("MP_ATTRIBUTE_NAME"),
                    "internal_name": record.get("MP_ATTRIBUTE_INTERNAL_NAME"),
                    "description": self.CREATION_TOOL
                }
                
                # Factory logic based on MP_ATTRIBUTE_FIELD_TYPE
                target_type = record.get("MP_ATTRIBUTE_FIELD_TYPE", "").strip()
                
                if target_type == "String":
                    default_string = record.get("default_value_string", "").strip() if isinstance(record.get("default_value_string"), str) else record.get("default_value_string")
                    if default_string:
                        data["default_value_string"] = default_string
                    
                    letters_max = self.convert_value(record.get("letters_max_count"), "int")
                    if letters_max is not None:
                        data["letters_max_count"] = letters_max
                
                elif target_type == "Integer":
                    min_int = self.convert_value(record.get("min_int_value"), "int")
                    if min_int is not None:
                        data["min_int_value"] = min_int
                    
                    max_int = self.convert_value(record.get("max_int_value"), "int")
                    if max_int is not None:
                        data["max_int_value"] = max_int
                    
                    default_int = self.convert_value(record.get("default_value_int"), "int")
                    if default_int is not None:
                        data["default_value_int"] = default_int
                    
                    percent = self.convert_value(record.get("percent"), "bool")
                    if percent is not None:
                        data["percent"] = percent
                
                elif target_type == "Decimal":
                    min_decimal = self.convert_value(record.get("min_decimal_value"), "float")
                    if min_decimal is not None:
                        data["min_decimal_value"] = min_decimal
                    
                    max_decimal = self.convert_value(record.get("max_decimal_value"), "float")
                    if max_decimal is not None:
                        data["max_decimal_value"] = max_decimal
                    
                    char_after_decimal = self.convert_value(record.get("characters_number_after_decimal_point"), "int")
                    if char_after_decimal is not None:
                        data["characters_number_after_decimal_point"] = char_after_decimal
                    
                    default_decimal = self.convert_value(record.get("default_value_decimal"), "float")
                    if default_decimal is not None:
                        data["default_value_decimal"] = default_decimal
                
                elif target_type == "Note":
                    change_lines = self.convert_value(record.get("change_lines_number"), "int")
                    if change_lines is not None:
                        data["change_lines_number"] = change_lines
                    
                    default_note = record.get("default_value_string", "").strip() if isinstance(record.get("default_value_string"), str) else record.get("default_value_string")
                    if default_note:
                        data["default_value_string"] = default_note
                    
                    row_count = self.convert_value(record.get("row_count"), "int")
                    if row_count is not None:
                        data["row_count"] = row_count
                    
                    rich_text = self.convert_value(record.get("rich_text"), "bool")
                    if rich_text is not None:
                        data["rich_text"] = rich_text
                    
                    rich_text_toolbar = self.convert_value(record.get("rich_text_toolbar_enabled"), "bool")
                    if rich_text_toolbar is not None:
                        data["rich_text_toolbar_enabled"] = rich_text_toolbar
                
                elif target_type == "Datetime":
                    default_date = self.convert_to_iso8601(record.get("default_value_date"))
                    if default_date:
                        data["default_value_date"] = default_date
                
                elif target_type == "Boolean":
                    default_bool = self.convert_value(record.get("default_value_boolean"), "bool")
                    if default_bool is not None:
                        data["default_value_boolean"] = default_bool
                
                elif target_type == "UUID":
                    default_guid = record.get("default_value_guid", "").strip() if isinstance(record.get("default_value_guid"), str) else record.get("default_value_guid")
                    if default_guid:
                        data["default_value_guid"] = default_guid
                
                elif target_type == "Lookup":
                    # display_field_list and extra_field_list are varchar in DB, send as strings
                    display_fields = record.get("display_field_list", "").strip()
                    if display_fields:
                        data["display_field_list"] = display_fields
                    
                    extra_fields = record.get("extra_field_list", "").strip()
                    if extra_fields:
                        data["extra_field_list"] = extra_fields
                    
                    # Optional scalar fields
                    default_guid = record.get("default_value_guid", "").strip()
                    if default_guid:
                        data["default_value_guid"] = default_guid
                    
                    lookup_list = record.get("list", "").strip()
                    if lookup_list:
                        data["list"] = lookup_list
                    
                    # Boolean and integer fields
                    multi = self.convert_value(record.get("multi"), "bool")
                    if multi is not None:
                        data["multi"] = multi
                    
                    search = self.convert_value(record.get("search"), "bool")
                    if search is not None:
                        data["search"] = search
                    
                    search_symbol_count = self.convert_value(record.get("search_symbol_count"), "int")
                    if search_symbol_count is not None:
                        data["search_symbol_count"] = search_symbol_count
                    
                    # Optional filter queries
                    filter_query = record.get("lookup_field_filter_query", "").strip()
                    if filter_query:
                        data["lookup_field_filter_query"] = filter_query
                    
                    list_filter_query = record.get("lookup_field_list_filter_query", "").strip()
                    if list_filter_query:
                        data["lookup_field_list_filter_query"] = list_filter_query
                    
                    # Optional view
                    view = record.get("view", "").strip()
                    if view:
                        data["view"] = view
                
                # Remove None values
                data = {k: v for k, v in data.items() if v is not None}
                
                # API call
                response = self.api_client.update_mp_list(data)
                
                if response and response.get('id'):
                    attr_id = response.get('id')
                    self.update_sheet_cell("ATTRIBUTES_UNIQUE", idx, "MP_ATTRIBUTE_ID", attr_id)
                    self.attr_cache[record.get("MP_ATTRIBUTE_NAME")] = attr_id
                    self.log_to_sheet("ATTRIBUTES_UNIQUE", idx, f"Attribute created: {attr_id}")
                else:
                    self.log_to_sheet("ATTRIBUTES_UNIQUE", idx, f"ERROR: {response}")
            
            except Exception as e:
                self.log_to_sheet("ATTRIBUTES_UNIQUE", idx, f"ERROR: {str(e)}")
        
        # Flush all queued batch updates
        time.sleep(self.google_api_delay)  # Rate limiting before flush
        self.flush_batch_updates()
    
    # ==================== STAGE 4: ADD ATTRIBUTES TO TYPES ====================
    def stage_4_add_attributes_to_types(self):
        """Stage 4: Add attributes to content types."""
        print("\n" + "="*60)
        print("STAGE 4: Adding Attributes to Types")
        print("="*60)
        
        time.sleep(self.google_api_delay)  # Rate limiting
        records = self.get_all_records("ATTRIBUTES")
        if not records:
            print("No records found in ATTRIBUTES sheet")
            return
        
        for idx, record in enumerate(records, start=2):
            try:
                # MIGRATION_APPROVED check
                if not self.convert_value(record.get("MIGRATION_APPROVED"), "bool"):
                    print(f"Row {idx}: Not approved for migration, skipping...")
                    continue
                
                # Skip if MP_ATTRIBUTE_INTERNAL_NAME == "name" or "Title"
                mp_internal_name = record.get("MP_ATTRIBUTE_INTERNAL_NAME", "").strip()
                if mp_internal_name.lower() == "name":
                    self.log_to_sheet("ATTRIBUTES", idx, "Skipped because MP_ATTRIBUTE_INTERNAL_NAME is 'name'")
                    continue
                
                # Idempotency check
                if self.is_idempotent_record(record, "SYNC_DONE"):
                    print(f"Row {idx}: Already synced, skipping...")
                    continue
                
                # Lookup values using XLOOKUP logic
                ctype_name = record.get("MP_CTYPE_NAME")
                attr_name = record.get("MP_ATTRIBUTE_NAME")
                
                # Try to get from cache, if not present skip for now
                if ctype_name not in self.ctype_cache:
                    self.log_to_sheet("ATTRIBUTES", idx, f"WARNING: Content type not found in cache: {ctype_name}")
                    continue
                
                if attr_name not in self.attr_cache:
                    self.log_to_sheet("ATTRIBUTES", idx, f"WARNING: Attribute not found in cache: {attr_name}")
                    continue
                
                ctype_id = self.ctype_cache[ctype_name]
                attr_id = self.attr_cache[attr_name]
                
                # Build payload
                data = {
                    "list_id": self.CONTENT_TYPE_FIELD_LIST_ID,
                    "content_type_id": self.CONTENT_TYPE_FIELD_CTYPE_ID,
                    "content_type": ctype_id,
                    "field": attr_id,
                    "required": self.convert_value(record.get("MP_ATTRIBUTE_IS_REQUIRED"), "bool"),
                    "read_only": self.convert_value(record.get("MP_ATTRIBUTE_IS_READ_ONLY"), "bool")
                }
                
                data = {k: v for k, v in data.items() if v is not None}
                
                # API call
                response = self.api_client.update_mp_list(data)
                
                if response and response.get('id'):
                    self.update_sheet_cell("ATTRIBUTES", idx, "SYNC_DONE", "SUCCESS")
                    self.log_to_sheet("ATTRIBUTES", idx, "Attribute added to type")
                else:
                    self.log_to_sheet("ATTRIBUTES", idx, f"ERROR: {response}")
            
            except Exception as e:
                self.log_to_sheet("ATTRIBUTES", idx, f"ERROR: {str(e)}")
        
        # Flush all queued batch updates
        time.sleep(self.google_api_delay)  # Rate limiting before flush
        self.flush_batch_updates()
    
    # ==================== STAGE 5: ADD CTYPES TO LISTS ====================
    def stage_5_add_ctypes_to_lists(self):
        """Stage 5: Add content types to lists."""
        print("\n" + "="*60)
        print("STAGE 5: Adding Types to Lists")
        print("="*60)
        
        time.sleep(self.google_api_delay)  # Rate limiting
        records = self.get_all_records("CTYPES")
        if not records:
            print("No records found in CTYPES sheet")
            return
        
        for idx, record in enumerate(records, start=2):
            try:
                # MIGRATION_APPROVED check
                if not self.convert_value(record.get("MIGRATION_APPROVED"), "bool"):
                    print(f"Row {idx}: Not approved for migration, skipping...")
                    continue
                
                # Idempotency check
                if self.is_idempotent_record(record, "SYNC_DONE"):
                    print(f"Row {idx}: Already synced, skipping...")
                    continue
                
                # Lookup values
                list_name = record.get("MP_LIST_NAME")
                ctype_name = record.get("MP_CTYPE_NAME")
                
                if list_name not in self.list_cache:
                    self.log_to_sheet("CTYPES", idx, f"WARNING: List not found in cache: {list_name}")
                    continue
                
                if ctype_name not in self.ctype_cache:
                    self.log_to_sheet("CTYPES", idx, f"WARNING: Content type not found in cache: {ctype_name}")
                    continue
                
                list_id = self.list_cache[list_name]
                ctype_id = self.ctype_cache[ctype_name]
                
                # Build payload
                data = {
                    "list_id": self.LIST_CONTENT_TYPE_LIST_ID,
                    "content_type_id": self.LIST_CONTENT_TYPE_CTYPE_ID,
                    "content_type": ctype_id,
                    "list": list_id
                }
                
                data = {k: v for k, v in data.items() if v is not None}
                
                # API call
                response = self.api_client.update_mp_list(data)
                
                if response and response.get('id'):
                    self.update_sheet_cell("CTYPES", idx, "SYNC_DONE", "SUCCESS")
                    self.log_to_sheet("CTYPES", idx, "Type added to list")
                else:
                    self.log_to_sheet("CTYPES", idx, f"ERROR: {response}")
            
            except Exception as e:
                self.log_to_sheet("CTYPES", idx, f"ERROR: {str(e)}")
        
        # Flush all queued batch updates
        time.sleep(self.google_api_delay)  # Rate limiting before flush
        self.flush_batch_updates()
    
    def run_all_stages(self):
        """Run all migration stages in sequence."""
        try:
            # Pre-load caches from existing data
            self.preload_caches()
            
            self.stage_1_create_lists()
            self.stage_2_create_ctypes()
            self.stage_3_create_attributes()
            self.stage_4_add_attributes_to_types()
            self.stage_5_add_ctypes_to_lists()
            
            print("\n" + "="*60)
            print("MIGRATION COMPLETE")
            print("="*60)
        
        except Exception as e:
            print(f"CRITICAL ERROR: {str(e)}")
        
        finally:
            self.api_client.close()


def main():
    """Main entry point."""
    # Get spreadsheet ID from environment
    spreadsheet_id = os.getenv('GOOGLE_SHEETS_ID')
    
    if not spreadsheet_id:
        print("ERROR: GOOGLE_SHEETS_ID not set in .env")
        return
    
    automation = VitroAutomation(spreadsheet_id)
    automation.run_all_stages()


if __name__ == '__main__':
    main()