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
        logger.error("❌ Error: SUPABASE_URL and SUPABASE_KEY environment variables must be set.")
        sys.exit(1)
    return create_client(supabase_url, supabase_key)

def fetch_all_rows(supabase: Client, table_name: str, batch_size: int = 1000) -> pd.DataFrame:
    """Fetches all rows from a Supabase table, handling pagination."""
    logger.info(f"Fetching latest data for '{table_name}'...")
    all_data = []
    offset = 0
    try:
        while True:
            response = supabase.table(table_name).select("*").range(offset, offset + batch_size - 1).execute()
            batch_data = response.data
            all_data.extend(batch_data)
            if len(batch_data) < batch_size:
                break
            offset += batch_size
        df = pd.DataFrame(all_data)
        logger.info(f"  > Fetched a total of {len(df)} rows.")
        return df
    except Exception as e:
        logger.error(f"An error occurred while fetching from {table_name}: {e}")
        return pd.DataFrame()

def main():
    """
    Fetches live gameweek status, updates the summary file, and then conditionally
    updates other master files if the current gameweek is not finished.
    """
    logger.info(f"--- Starting GitHub Actions Data Update for Season {SEASON_NAME} ---")
    logger.info(f"Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    supabase = initialize_supabase_client()

    # --- 1. Fetch Live Gameweek Data to Get the True Current Status ---
    gameweeks_df = fetch_all_rows(supabase, 'gameweeks')
    if gameweeks_df.empty:
        logger.error("❌ Critical: Could not fetch gameweek data. Aborting process.")
        sys.exit(1)

    # --- 2. Always Update the Gameweek Summary File ---
    # This ensures the repo's summary file is always the latest version.
    logger.info("\nUpdating 'gameweek_summaries.csv' with the latest data...")
    os.makedirs(BASE_DATA_PATH, exist_ok=True)
    gameweeks_df.to_csv(os.path.join(BASE_DATA_PATH, 'gameweek_summaries.csv'), index=False)
    logger.info("  > 'gameweek_summaries.csv' successfully updated.")

    # --- 3. Check the Live Status of the Current Gameweek ---
    current_gw_df = gameweeks_df[gameweeks_df['is_current'] == True]
    if current_gw_df.empty:
        logger.warning("⚠️ Warning: No current gameweek found in fetched data. No further action will be taken.")
        sys.exit(0)
    
    current_gw_info = current_gw_df.iloc[0]
    gameweek_id = int(current_gw_info['id'])
    is_finished = current_gw_info['finished']
    
    # --- 4. Decide Whether to Update Other Files ---
    if is_finished:
        logger.info(f"\n✅ Gameweek {gameweek_id} is marked as finished. No further data updates are necessary.")
        logger.info("--- Process complete. ---")
        sys.exit(0)
    else:
        logger.info(f"\n Gameweek {gameweek_id} is active. Proceeding to update master data files...")

    # --- 5. Fetch and Overwrite Other Master Files ---
    players_df = fetch_all_rows(supabase, 'players')
    playerstats_df = fetch_all_rows(supabase, 'playerstats')
    teams_df = fetch_all_rows(supabase, 'teams')
    
    if any(df.empty for df in [players_df, playerstats_df, teams_df]):
        logger.error("❌ Aborting: One or more required tables (players, playerstats, teams) could not be fetched.")
        sys.exit(1)
        
    logger.info("\nOverwriting master CSV files with fresh data...")
    players_df.to_csv(os.path.join(BASE_DATA_PATH, 'players.csv'), index=False)
    playerstats_df.to_csv(os.path.join(BASE_DATA_PATH, 'playerstats.csv'), index=False)
    teams_df.to_csv(os.path.join(BASE_DATA_PATH, 'teams.csv'), index=False)
    logger.info("  > Master files updated successfully.")

    logger.info("\n--- Automated data update process completed successfully! ---")

if __name__ == "__main__":
    main()
