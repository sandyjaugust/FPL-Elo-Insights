import os
import sys
import pandas as pd
from supabase import create_client, Client
import logging
from datetime import datetime, timezone

# --- Configuration ---
SEASON_NAME = "2025-2026"
BASE_DATA_PATH = os.path.join('data', SEASON_NAME)
SUMMARY_FILE_PATH = os.path.join(BASE_DATA_PATH, 'gameweek_summaries.csv')

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

def get_start_gameweek() -> int:
    """
    Determines the starting gameweek for the incremental load.
    If a local summary file exists, it uses the latest finished gameweek.
    Otherwise, it starts from gameweek 1.
    """
    if os.path.exists(SUMMARY_FILE_PATH):
        logger.info(f"Local '{os.path.basename(SUMMARY_FILE_PATH)}' found. Determining start gameweek from local file.")
        try:
            summaries_df = pd.read_csv(SUMMARY_FILE_PATH)
            # Find the last gameweek that is marked as finished
            finished_gws = summaries_df[summaries_df['finished'] == True]
            if not finished_gws.empty:
                # *** BUG FIX 1: Use 'id' column from gameweeks table, not 'gw' ***
                latest_finished_gw = finished_gws['id'].max()
                start_gw = int(latest_finished_gw) + 1
                logger.info(f"  > Latest FINISHED gameweek in local file: {latest_finished_gw}. Processing from GW{start_gw}.")
                return start_gw
        except KeyError:
             logger.warning(f"Could not find 'id' or 'finished' column in summary file. Defaulting to full load from GW1.")
        except Exception as e:
            logger.warning(f"Could not process local summary file: {e}. Defaulting to full load from GW1.")
    
    logger.info("No valid local summary file found. Starting full process from GW1.")
    return 1

def fetch_data_from_table(supabase: Client, table_name: str, start_gw: int = None) -> pd.DataFrame:
    """
    Fetches data from a specified Supabase table.
    Performs an incremental load if start_gw is provided and the table has a 'gw' column.
    """
    is_incremental = start_gw is not None and start_gw > 1 and table_name in ['matches', 'playerstats']
    
    if is_incremental:
        logger.info(f"Fetching data from '{table_name}' for GW{start_gw} onwards (Incremental Load)...")
        query = supabase.table(table_name).select("*").gte('gw', int(start_gw))
    else:
        logger.info(f"Fetching all records from master table: '{table_name}'...")
        query = supabase.table(table_name).select("*")

    try:
        response = query.execute()
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
    start_gameweek = get_start_gameweek()

    # --- Fetch Master & Incremental Data ---
    players_df = fetch_data_from_table(supabase, 'players')
    teams_df = fetch_data_from_table(supabase, 'teams')
    gameweeks_df = fetch_data_from_table(supabase, 'gameweeks')
    
    logger.info(f"\n--- Processing data for Gameweek {start_gameweek} and onwards ---")
    matches_df = fetch_data_from_table(supabase, 'matches', start_gameweek)
    playerstats_df = fetch_data_from_table(supabase, 'playerstats', start_gameweek)
    
    if playerstats_df.empty and start_gameweek > 1:
        logger.info("No new playerstats data found since last run. Process complete.")
        return

    logger.info("\n--- Pre-processing fetched data ---")
    new_gameweeks = sorted(playerstats_df['gw'].unique())
    logger.info(f"Found data for new gameweeks: {new_gameweeks}\n")

    # --- Process and Save Data for Each Gameweek ---
    for gw in new_gameweeks:
        logger.info(f"--- Saving data for GW{gw} ---")
        
        # *** BUG FIX 2: Check if gw exists in gameweeks_df before processing ***
        gw_info_df = gameweeks_df[gameweeks_df['id'] == gw]
        if gw_info_df.empty:
            logger.warning(f"  > No official event found for GW{gw}. Skipping this gameweek.")
            continue
        
        gw_info = gw_info_df.iloc[0]
        gw_playerstats = playerstats_df[playerstats_df['gw'] == gw]
        gw_matches = matches_df[matches_df['event'] == gw]
        
        gw_base_path = os.path.join(BASE_DATA_PATH, 'By Gameweek', f'GW{gw}')
        tournament_path = os.path.join(BASE_DATA_PATH, 'By Tournament', 'Premier League', f'GW{gw}')
        os.makedirs(gw_base_path, exist_ok=True)
        os.makedirs(tournament_path, exist_ok=True)

        gw_playerstats.to_csv(os.path.join(gw_base_path, 'player_stats.csv'), index=False)
        gw_matches.to_csv(os.path.join(gw_base_path, 'matches.csv'), index=False)
        
        if not gw_info['finished']:
            logger.info("  > Gameweek is not finished. Saving current player/team snapshots.")
            players_df.to_csv(os.path.join(gw_base_path, 'players.csv'), index=False)
            teams_df.to_csv(os.path.join(gw_base_path, 'teams.csv'), index=False)
        else:
             logger.info("  > Gameweek is finished. Skipping player/team snapshot update to preserve history.")

        logger.info(f"  > Saved data to '{gw_base_path}'")
        logger.info(f"  > Saved data to '{tournament_path}'\n")

    # --- Update Master Files ---
    logger.info("--- Updating master data files in root directory ---")
    finished_gws_in_run = [gw for gw in new_gameweeks if not gameweeks_df[gameweeks_df['id'] == gw].empty and gameweeks_df[gameweeks_df['id'] == gw].iloc[0]['finished']]
    
    if finished_gws_in_run:
        logger.info(f"  > Updating master 'playerstats.csv' with data for finished GWs: {finished_gws_in_run}")
        master_stats_path = os.path.join(BASE_DATA_PATH, 'playerstats.csv')
        new_finished_stats = playerstats_df[playerstats_df['gw'].isin(finished_gws_in_run)]
        
        if os.path.exists(master_stats_path):
            master_stats_df = pd.read_csv(master_stats_path)
            master_stats_df = master_stats_df[~master_stats_df['gw'].isin(finished_gws_in_run)]
            updated_df = pd.concat([master_stats_df, new_finished_stats], ignore_index=True)
        else:
            updated_df = new_finished_stats
        
        updated_df.to_csv(master_stats_path, index=False)

    # Update the gameweek summaries file
    gameweeks_df.to_csv(SUMMARY_FILE_PATH, index=False)
    logger.info(f"  > Master files in '{BASE_DATA_PATH}' updated.")

    logger.info("\n--- Automated data update process completed successfully! ---")

if __name__ == "__main__":
    main()
