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
    format='%(message)s' # Simplified format to match user's logs
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

def fetch_full_table(supabase: Client, table_name: str) -> pd.DataFrame:
    """Fetches all data from a specified Supabase table."""
    logger.info(f"Fetching all records from table: '{table_name}'...")
    try:
        response = supabase.table(table_name).select("*").execute()
        df = pd.DataFrame(response.data)
        logger.info(f"  > Fetched {len(df)} total rows from '{table_name}'.")
        return df
    except Exception as e:
        logger.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

def main():
    """Main function to run the data export and processing pipeline."""
    logger.info(f"--- Starting Automated Data Update for Season {SEASON_NAME} ---")
    logger.info(f"Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    supabase = initialize_supabase_client()

    # --- 1. Fetch All Data from Supabase ---
    players_df = fetch_full_table(supabase, 'players')
    teams_df = fetch_full_table(supabase, 'teams')
    gameweeks_df = fetch_full_table(supabase, 'gameweeks')
    matches_df = fetch_full_table(supabase, 'matches')
    playerstats_df = fetch_full_table(supabase, 'playerstats')
    playermatchstats_df = fetch_full_table(supabase, 'playermatchstats') # New table
    
    # Exit if essential data is missing
    if any(df.empty for df in [players_df, teams_df, gameweeks_df, playerstats_df]):
        logger.error("One or more essential tables could not be fetched. Aborting.")
        sys.exit(1)

    # --- 2. Update All Master CSV Files ---
    logger.info("\n--- Overwriting master data files with the latest data ---")
    os.makedirs(BASE_DATA_PATH, exist_ok=True)
    players_df.to_csv(os.path.join(BASE_DATA_PATH, 'players.csv'), index=False)
    teams_df.to_csv(os.path.join(BASE_DATA_PATH, 'teams.csv'), index=False)
    playerstats_df.to_csv(os.path.join(BASE_DATA_PATH, 'playerstats.csv'), index=False)
    gameweeks_df.to_csv(os.path.join(BASE_DATA_PATH, 'gameweek_summaries.csv'), index=False)
    logger.info("  > Master files updated successfully.")

    # --- 3. Process and Save Data for Each Gameweek Folder ---
    logger.info("\n--- Populating individual gameweek folders based on status ---")
    
    # Determine the latest gameweek to process (current or last finished)
    current_gw_series = gameweeks_df[gameweeks_df['is_current'] == True]
    if not current_gw_series.empty:
        latest_gameweek_to_process = current_gw_series['id'].iloc[0]
    else:
        # Fallback if no GW is current (e.g., between seasons)
        latest_gameweek_to_process = gameweeks_df[gameweeks_df['finished'] == True]['id'].max()

    for gw in range(1, int(latest_gameweek_to_process) + 1):
        gw_info_df = gameweeks_df[gameweeks_df['id'] == gw]
        if gw_info_df.empty:
            continue
        
        gw_info = gw_info_df.iloc[0]
        gw_base_path = os.path.join(BASE_DATA_PATH, 'By Gameweek', f'GW{gw}')
        os.makedirs(gw_base_path, exist_ok=True)

        logger.info(f"Processing GW{gw} (Finished: {gw_info['finished']})...")
        
        # Filter data for the current gameweek
        gw_matches = matches_df[matches_df['gameweek'] == gw]

        if not gw_info['finished']:
            # For UNFINISHED gameweeks, save current stats and master lists
            logger.info("  > Saving playerstats, players, teams, and fixtures snapshots.")
            gw_playerstats = playerstats_df[playerstats_df['gw'] == gw]
            
            gw_playerstats.to_csv(os.path.join(gw_base_path, 'playerstats.csv'), index=False)
            players_df.to_csv(os.path.join(gw_base_path, 'players.csv'), index=False)
            teams_df.to_csv(os.path.join(gw_base_path, 'teams.csv'), index=False)
            gw_matches.to_csv(os.path.join(gw_base_path, 'fixtures.csv'), index=False)

        else:
            # For FINISHED gameweeks, save final match results and detailed stats
            logger.info("  > Saving final matches and playermatchstats.")
            # *** BUG FIX: Use 'gw' column for playermatchstats, not 'gameweek' ***
            gw_playermatchstats = playermatchstats_df[playermatchstats_df['gw'] == gw]
            
            gw_matches.to_csv(os.path.join(gw_base_path, 'matches.csv'), index=False)
            gw_playermatchstats.to_csv(os.path.join(gw_base_path, 'playermatchstats.csv'), index=False)

    logger.info("\n--- Automated data update process completed successfully! ---")

if __name__ == "__main__":
    main()
