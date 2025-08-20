import os
import sys
import pandas as pd
from supabase import create_client, Client
import logging
from datetime import datetime, timezone

# --- Configuration ---
SEASON = "2025-2026"
BASE_DATA_PATH = os.path.join('data', SEASON)
TOURNAMENT_NAME_MAP = {
    'friendly': 'Friendlies',
    'premier-league': 'Premier League',
    'champions-league': 'Champions League',
    'prem': 'Premier League',
    'community-shield': 'Community Shield',
    'uefa-super-cup': 'Uefa Super Cup'
}

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def initialize_supabase_client() -> Client:
    """Initializes and returns a Supabase client."""
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        logger.error("❌ Error: SUPABASE_URL and SUPABASE_KEY must be set.")
        sys.exit(1)
    return create_client(supabase_url, supabase_key)

def fetch_all_rows(supabase: Client, table_name: str) -> pd.DataFrame:
    """Fetches all rows from a Supabase table, handling pagination."""
    logger.info(f"Fetching latest data for '{table_name}'...")
    all_data = []
    offset = 0
    try:
        while True:
            response = supabase.table(table_name).select("*").range(offset, offset + 1000 - 1).execute()
            batch_data = response.data
            all_data.extend(batch_data)
            if len(batch_data) < 1000:
                break
            offset += 1000
        df = pd.DataFrame(all_data)
        logger.info(f"  > Fetched a total of {len(df)} rows.")
        return df
    except Exception as e:
        logger.error(f"An error occurred while fetching from {table_name}: {e}")
        return pd.DataFrame()

def main():
    """Runs the full, corrected data export pipeline."""
    logger.info(f"--- Starting Comprehensive Data Update for Season {SEASON} ---")
    logger.info(f"Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    supabase = initialize_supabase_client()

    # --- Fetch ALL data at the beginning ---
    gameweeks_df = fetch_all_rows(supabase, 'gameweeks')
    players_df = fetch_all_rows(supabase, 'players')
    playerstats_df = fetch_all_rows(supabase, 'playerstats')
    teams_df = fetch_all_rows(supabase, 'teams')
    matches_df = fetch_all_rows(supabase, 'matches')
    playermatchstats_df = fetch_all_rows(supabase, 'playermatchstats')

    essential_dfs = [gameweeks_df, players_df, playerstats_df, teams_df, matches_df]
    if any(df.empty for df in essential_dfs):
        logger.error("❌ Critical: One or more essential tables could not be fetched. Aborting.")
        sys.exit(1)

    # --- Extract tournament slug from match_id ---
    def extract_tournament_slug(match_id):
        for slug in TOURNAMENT_NAME_MAP.keys():
            if slug in match_id:
                return slug
        return None
    matches_df['tournament'] = matches_df['match_id'].apply(extract_tournament_slug)

    # --- 1. Update Master Data Files (Unconditional) ---
    logger.info("\n--- 1. Updating Master Data Files ---")
    os.makedirs(BASE_DATA_PATH, exist_ok=True)
    gameweeks_df.to_csv(os.path.join(BASE_DATA_PATH, 'gameweek_summaries.csv'), index=False)
    players_df.to_csv(os.path.join(BASE_DATA_PATH, 'players.csv'), index=False)
    playerstats_df.to_csv(os.path.join(BASE_DATA_PATH, 'playerstats.csv'), index=False)
    teams_df.to_csv(os.path.join(BASE_DATA_PATH, 'teams.csv'), index=False)
    logger.info("  > Master files updated successfully.")

    # --- 2. Populate 'By Tournament' Folders ---
    logger.info("\n--- 2. Populating 'By Tournament' Folders ---")
    unique_tournaments = matches_df['tournament'].dropna().unique()
    for slug in unique_tournaments:
        folder_name = TOURNAMENT_NAME_MAP.get(slug, slug.replace('-', ' ').title())
        logger.info(f"Processing Tournament: {folder_name}...")
        
        tournament_matches = matches_df[matches_df['tournament'] == slug]
        gws_in_tournament = sorted(tournament_matches['gameweek'].dropna().unique().astype(int))

        for gw in gws_in_tournament:
            is_finished = gameweeks_df.loc[gameweeks_df['id'] == gw, 'finished'].iloc[0]
            logger.info(f"  > Processing GW{gw} for {folder_name} (Finished: {is_finished})...")
            
            tournament_gw_path = os.path.join(BASE_DATA_PATH, 'By Tournament', folder_name, f'GW{gw}')
            os.makedirs(tournament_gw_path, exist_ok=True)
            
            gw_tournament_matches = tournament_matches[tournament_matches['gameweek'] == gw]
            match_ids = gw_tournament_matches['match_id'].unique().tolist()
            gw_tournament_playerstats = playermatchstats_df[playermatchstats_df['match_id'].isin(match_ids)]
            
            # Save matches and playermatchstats for ALL gameweeks (in-progress or finished)
            gw_tournament_matches.to_csv(os.path.join(tournament_gw_path, 'matches.csv'), index=False)
            gw_tournament_playerstats.to_csv(os.path.join(tournament_gw_path, 'playermatchstats.csv'), index=False)

            # For active gameweeks, ALSO save fixtures and snapshots
            if not is_finished:
                gw_tournament_matches.to_csv(os.path.join(tournament_gw_path, 'fixtures.csv'), index=False)
                players_df.to_csv(os.path.join(tournament_gw_path, 'players.csv'), index=False)
                teams_df.to_csv(os.path.join(tournament_gw_path, 'teams.csv'), index=False)
                gw_playerstats_snapshot = playerstats_df[playerstats_df['gw'] == gw]
                gw_playerstats_snapshot.to_csv(os.path.join(tournament_gw_path, 'playerstats.csv'), index=False)

    # --- 3. Populate 'By Gameweek' Folders ---
    logger.info("\n--- 3. Populating 'By Gameweek' Folders ---")
    unique_gameweeks = sorted(gameweeks_df['id'].dropna().unique().astype(int))

    for gw in unique_gameweeks:
        is_finished = gameweeks_df.loc[gameweeks_df['id'] == gw, 'finished'].iloc[0]
        logger.info(f"Processing GW{gw} (Finished: {is_finished})...")

        gw_path = os.path.join(BASE_DATA_PATH, 'By Gameweek', f'GW{gw}')
        os.makedirs(gw_path, exist_ok=True)
        
        gw_matches = matches_df[matches_df['gameweek'] == gw]
        match_ids = gw_matches['match_id'].unique().tolist()
        gw_playermatchstats = playermatchstats_df[playermatchstats_df['match_id'].isin(match_ids)]
        
        # Save matches and playermatchstats for ALL gameweeks (in-progress or finished)
        logger.info("  > Saving matches and player-match-stats...")
        gw_matches.to_csv(os.path.join(gw_path, 'matches.csv'), index=False)
        gw_playermatchstats.to_csv(os.path.join(gw_path, 'playermatchstats.csv'), index=False)
        
        # For active gameweeks, ALSO save fixtures and data snapshots
        if not is_finished:
            logger.info("  > Gameweek is active. Saving fixtures and data snapshots...")
            gw_matches.to_csv(os.path.join(gw_path, 'fixtures.csv'), index=False)
            players_df.to_csv(os.path.join(gw_path, 'players.csv'), index=False)
            teams_df.to_csv(os.path.join(gw_path, 'teams.csv'), index=False)
            
            gw_playerstats_snapshot = playerstats_df[playerstats_df['gw'] == gw]
            gw_playerstats_snapshot.to_csv(os.path.join(gw_path, 'playerstats.csv'), index=False)

    logger.info("\n--- Comprehensive data update process completed successfully! ---")

if __name__ == "__main__":
    main()
