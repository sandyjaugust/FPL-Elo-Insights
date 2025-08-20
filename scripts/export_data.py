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
            response = supabase.table(table_name).select("*").range(offset, offset + batch_size - 1).execute()
            batch_data = response.data
            all_data.extend(batch_data)
            
            if len(batch_data) < batch_size:
                break # Reached the end
            offset += batch_size
        
        df = pd.DataFrame(all_data)
        logger.info(f"  > Finished. Fetched a total of {len(df)} rows from '{table_name}'.")
        return df
    except Exception as e:
        logger.error(f"An error occurred while fetching from {table_name}: {e}")
        return pd.DataFrame()

def main():
    """
    Main function to fetch match data and organize it by Tournament and Gameweek.
    """
    logger.info(f"--- Starting Automated Data Update for Season {SEASON_NAME} ---")
    logger.info(f"Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    supabase = initialize_supabase_client()

    # --- 1. Fetch All Required Data from Supabase ---
    # We assume 'matches' table has a 'tournament' foreign key and 'gameweek' column.
    matches_df = fetch_all_rows(supabase, 'matches')
    playermatchstats_df = fetch_all_rows(supabase, 'playermatchstats')
    tournaments_df = fetch_all_rows(supabase, 'tournaments')
    
    # Exit if essential data is missing
    if any(df.empty for df in [matches_df, playermatchstats_df, tournaments_df]):
        logger.error("Could not fetch essential tables (matches, playermatchstats, tournaments). Aborting.")
        sys.exit(1)

    # --- 2. Process and Save Data by Tournament ---
    logger.info("\n--- Populating 'By Tournament' folders ---")
    for _, tournament in tournaments_df.iterrows():
        tournament_id = tournament['id']
        tournament_name = tournament['name']
        
        logger.info(f"Processing Tournament: {tournament_name}...")
        
        # Create directory path
        tournament_path = os.path.join(BASE_DATA_PATH, 'By Tournament', tournament_name)
        os.makedirs(tournament_path, exist_ok=True)
        
        # Filter matches for the current tournament
        tournament_matches = matches_df[matches_df['tournament'] == tournament_id]
        tournament_matches.to_csv(os.path.join(tournament_path, 'matches.csv'), index=False)
        
        # Filter player stats for the matches in this tournament
        match_ids_for_tournament = tournament_matches['match_id'].unique().tolist()
        tournament_playerstats = playermatchstats_df[playermatchstats_df['match_id'].isin(match_ids_for_tournament)]
        tournament_playerstats.to_csv(os.path.join(tournament_path, 'playermatchstats.csv'), index=False)
        
        logger.info(f"  > Saved {len(tournament_matches)} matches and {len(tournament_playerstats)} player stat entries.")

    # --- 3. Process and Save Data by Gameweek ---
    logger.info("\n--- Populating 'By Gameweek' folders ---")
    unique_gameweeks = sorted(matches_df['gameweek'].dropna().unique().astype(int))
    
    for gw in unique_gameweeks:
        logger.info(f"Processing GW{gw}...")
        
        # Create directory path
        gw_path = os.path.join(BASE_DATA_PATH, 'By Gameweek', f'GW{gw}')
        os.makedirs(gw_path, exist_ok=True)
        
        # Filter matches for the current gameweek
        gw_matches = matches_df[matches_df['gameweek'] == gw]
        gw_matches.to_csv(os.path.join(gw_path, 'matches.csv'), index=False)
        
        # Filter player stats for the matches in this gameweek
        match_ids_for_gw = gw_matches['match_id'].unique().tolist()
        gw_playerstats = playermatchstats_df[playermatchstats_df['match_id'].isin(match_ids_for_gw)]
        gw_playerstats.to_csv(os.path.join(gw_path, 'playermatchstats.csv'), index=False)
        
        logger.info(f"  > Saved {len(gw_matches)} matches and {len(gw_playerstats)} player stat entries.")

    logger.info("\n--- Automated data update process completed successfully! ---")

if __name__ == "__main__":
    main()
