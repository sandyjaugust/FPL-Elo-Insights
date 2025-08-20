import os
import sys
import pandas as pd
from supabase import create_client, Client
import logging
from datetime import datetime, timezone

# --- Configuration ---
SEASON_NAME = "2025-2026"
BASE_DATA_PATH = os.path.join('data', SEASON_NAME)

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s' # Simplified format
)
logger = logging.getLogger(__name__)

def initialize_supabase_client() -> Client:
    """Initializes and returns a Supabase client using environment variables."""
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        logger.error("Error: SUPABASE_URL and SUPABASE_KEY environment variables must be set.")
        sys.exit(1)
    return create_client(supabase_url, supabase_key)

def fetch_all_rows(supabase: Client, table_name: str, batch_size: int = 1000) -> pd.DataFrame:
    """
    Fetches all rows from a Supabase table, handling pagination for large tables.
    """
    logger.info(f"Fetching all records from table: '{table_name}'...")
    all_data = []
    offset = 0
    try:
        while True:
            # Fetch a batch of rows
            start_index = offset
            end_index = offset + batch_size - 1
            
            response = supabase.table(table_name).select("*").range(start_index, end_index).execute()
            
            batch_data = response.data
            all_data.extend(batch_data)
            
            logger.info(f"  > Fetched {len(batch_data)} rows (total: {len(all_data)})...")

            # If the number of rows returned is less than the batch size, we've reached the end
            if len(batch_data) < batch_size:
                break
            
            # Move to the next page
            offset += batch_size
        
        df = pd.DataFrame(all_data)
        logger.info(f"  > Finished. Fetched a total of {len(df)} rows from '{table_name}'.")
        return df

    except Exception as e:
        logger.error(f"An error occurred while fetching from {table_name}: {e}")
        return pd.DataFrame()


def main():
    """
    Main function to fetch full data tables from Supabase and save them as master CSV files.
    """
    logger.info(f"--- Starting Automated Data Update for Season {SEASON_NAME} ---")
    logger.info(f"Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    supabase = initialize_supabase_client()

    # --- 1. Fetch All Data from Supabase (now handles large tables) ---
    players_df = fetch_all_rows(supabase, 'players')
    teams_df = fetch_all_rows(supabase, 'teams')
    gameweeks_df = fetch_all_rows(supabase, 'gameweeks')
    playerstats_df = fetch_all_rows(supabase, 'playerstats')
    
    # Exit if any essential data is missing
    if any(df.empty for df in [players_df, teams_df, gameweeks_df, playerstats_df]):
        logger.error("One or more essential tables could not be fetched. Aborting.")
        sys.exit(1)

    # --- 2. Overwrite Master CSV Files ---
    logger.info("\n--- Overwriting master data files with the latest data ---")
    os.makedirs(BASE_DATA_PATH, exist_ok=True)
    
    players_df.to_csv(os.path.join(BASE_DATA_PATH, 'players.csv'), index=False)
    teams_df.to_csv(os.path.join(BASE_DATA_PATH, 'teams.csv'), index=False)
    playerstats_df.to_csv(os.path.join(BASE_DATA_PATH, 'playerstats.csv'), index=False)
    gameweeks_df.to_csv(os.path.join(BASE_DATA_PATH, 'gameweek_summaries.csv'), index=False)
    
    logger.info("  > Master files updated successfully.")
    logger.info("\n--- Automated data update process completed successfully! ---")

if __name__ == "__main__":
    main()
