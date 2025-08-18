import os
import pandas as pd
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv
from pathlib import Path

# --- Configuration ---
SEASON = "2025-2026"
TOURNAMENT_NAME_MAP = {
    'friendly': 'Friendlies',
    'premier-league': 'Premier League',
    'champions-league': 'Champions League',
    'prem': 'Premier League',
    'community-shield': 'Community Shield',
    'uefa-super-cup' : 'Uefa Super Cup'
}

# --- Setup ---
load_dotenv()
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("FATAL ERROR: SUPABASE_URL and SUPABASE_KEY must be set.")
    exit()

supabase: Client = create_client(url, key)

# --- Helper Functions ---

def create_directory(path: str):
    Path(path).mkdir(parents=True, exist_ok=True)

def get_tournament_name_from_id(match_id: str, name_map: dict) -> str:
    for slug, name in sorted(name_map.items(), key=lambda item: len(item[0]), reverse=True):
        if slug in match_id:
            return name
    return "Other"

def fetch_all_records(table_name: str) -> pd.DataFrame:
    """Fetches all records from a table, handling pagination."""
    print(f"Fetching all records from master table: '{table_name}'...")
    all_data = []
    offset = 0
    chunk_size = 1000
    try:
        while True:
            response = supabase.table(table_name).select('*').range(offset, offset + chunk_size - 1).execute()
            chunk_data = response.data
            all_data.extend(chunk_data)
            if len(chunk_data) < chunk_size: break
            offset += chunk_size
        df = pd.DataFrame(all_data)
        print(f"  > Fetched {len(df)} total rows from '{table_name}'.")
        return df
    except Exception as e:
        print(f"  ERROR fetching from '{table_name}': {e}")
        return pd.DataFrame()

def get_latest_gameweek_from_table(table_name: str, gameweek_col: str = 'gameweek', finished_only: bool = True) -> int:
    """Finds the highest gameweek number from a specified table."""
    print(f"Querying database for the latest gameweek in '{table_name}'...")
    try:
        query = supabase.table(table_name).select(gameweek_col)
        if finished_only: query = query.eq('finished', True)
        response = query.execute()
        if not response.data:
            print(f"  > No gameweeks found. Defaulting to 1.")
            return 1
        valid_gameweeks = [item[gameweek_col] for item in response.data if item.get(gameweek_col) is not None]
        if not valid_gameweeks:
            print(f"  > No valid gameweek numbers found. Defaulting to 1.")
            return 1
        latest_gw = max(valid_gameweeks)
        print(f"  > Latest finished gameweek found in '{table_name}': {latest_gw}.")
        return latest_gw
    except Exception as e:
        print(f"  ERROR: Could not fetch latest gameweek from '{table_name}': {e}. Defaulting to 1.")
        return 1

def fetch_data_since_gameweek(table_name: str, start_gameweek: int, gameweek_col: str = 'gameweek') -> pd.DataFrame:
    """Fetches data from a table from a specific gameweek onwards."""
    print(f"Fetching data from '{table_name}' for GW{start_gameweek} onwards (Incremental Load)...")
    try:
        response = supabase.table(table_name).select('*').gte(gameweek_col, start_gameweek).execute()
        df = pd.DataFrame(response.data)
        print(f"  > Fetched {len(df)} new/updated rows from '{table_name}'.")
        return df
    except Exception as e:
        print(f"  ERROR fetching from '{table_name}': {e}")
        return pd.DataFrame()

def fetch_data_by_ids(table_name: str, column: str, ids: list) -> pd.DataFrame:
    if not ids: return pd.DataFrame()
    print(f"Fetching {len(ids)} related records from '{table_name}' using '{column}'...")
    all_data = []
    chunk_size = 500
    for i in range(0, len(ids), chunk_size):
        chunk_ids = ids[i:i + chunk_size]
        try:
            response = supabase.table(table_name).select('*').in_(column, chunk_ids).execute()
            all_data.extend(response.data)
        except Exception as 
