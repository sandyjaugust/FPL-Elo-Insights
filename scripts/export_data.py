# - Hi future me, remember this wont pull friendlies or gameweek: 0, a value that doesn't exist in your main gameweeks table. So ive filter them out remember when youre working on that next season

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
    'uefa-super-cup': 'Uefa Super Cup',
    'efl-cup' : 'EFL Cup'
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

### NEW ###
# --- Section for calculating discrete gameweek stats ---

# Column definitions for the calculation logic
CUMULATIVE_COLS = [
    'total_points', 'minutes', 'goals_scored', 'assists', 'clean_sheets',
    'goals_conceded', 'own_goals', 'penalties_saved', 'penalties_missed',
    'yellow_cards', 'red_cards', 'saves', 'starts', 'bonus', 'bps',
    'transfers_in', 'transfers_out', 'dreamteam_count', 'expected_goals',
    'expected_assists', 'expected_goal_involvements', 'expected_goals_conceded',
    'influence', 'creativity', 'threat', 'ict_index'
]
ID_COLS = ['id', 'first_name', 'second_name', 'web_name']
SNAPSHOT_COLS = [
    'status', 'news', 'now_cost', 'selected_by_percent', 'form', 'event_points',
    'cost_change_event', 'transfers_in_event', 'transfers_out_event',
    'value_form', 'value_season', 'ep_next', 'ep_this'
]

def calculate_discrete_gameweek_stats():
    """
    Iterates through gameweek folders, calculates discrete stats from the
    cumulative playerstats files, and saves the result to a new CSV.
    """
    logger.info("\n--- 4. Calculating and Saving Discrete Gameweek Player Stats ---")
    by_gameweek_path = os.path.join(BASE_DATA_PATH, 'By Gameweek')
    output_filename = 'player_gameweek_stats.csv'

    if not os.path.isdir(by_gameweek_path):
        logger.error(f"  > Directory not found: '{by_gameweek_path}'. Skipping calculation.")
        return

    try:
        gameweek_dirs = sorted(
            [d for d in os.listdir(by_gameweek_path) if d.startswith('GW')],
            key=lambda x: int(x[2:])
        )
    except (ValueError, IndexError):
        logger.error("  > Could not parse gameweek numbers from directory names. Skipping.")
        return

    if not gameweek_dirs:
        logger.warning("  > No gameweek directories found to process.")
        return

    # Process Gameweek 1 (Baseline)
    gw1_dir = gameweek_dirs[0]
    logger.info(f"Processing baseline: {gw1_dir}...")
    gw1_path = os.path.join(by_gameweek_path, gw1_dir, 'playerstats.csv')

    if os.path.exists(gw1_path):
        gw1_df = pd.read_csv(gw1_path)
        final_cols = ID_COLS + SNAPSHOT_COLS + CUMULATIVE_COLS
        existing_cols = [col for col in final_cols if col in gw1_df.columns]
        gw1_output_df = gw1_df[existing_cols]
        output_path = os.path.join(by_gameweek_path, gw1_dir, output_filename)
        gw1_output_df.to_csv(output_path, index=False)
        logger.info(f"  > Saved baseline stats to '{output_filename}'")
    else:
        logger.warning(f"  > playerstats.csv not found for {gw1_dir}, skipping.")

    # Process Gameweek 2 onwards
    for i in range(1, len(gameweek_dirs)):
        prev_gw_dir = gameweek_dirs[i-1]
        current_gw_dir = gameweek_dirs[i]
        logger.info(f"Processing {current_gw_dir} (comparing with {prev_gw_dir})...")

        prev_stats_path = os.path.join(by_gameweek_path, prev_gw_dir, 'playerstats.csv')
        current_stats_path = os.path.join(by_gameweek_path, current_gw_dir, 'playerstats.csv')

        if not os.path.exists(current_stats_path) or not os.path.exists(prev_stats_path):
            logger.warning(f"  > Missing a playerstats.csv file for calculation. Skipping.")
            continue

        prev_df = pd.read_csv(prev_stats_path)
        current_df = pd.read_csv(current_stats_path)
        
        merged_df = pd.merge(
            current_df,
            prev_df[ID_COLS + CUMULATIVE_COLS],
            on='id', how='left', suffixes=('', '_prev')
        )
        
        for col in CUMULATIVE_COLS:
            if col in merged_df.columns and f"{col}_prev" in merged_df.columns:
                merged_df[f"{col}_prev"] = merged_df[f"{col}_prev"].fillna(0)
                merged_df[col] = merged_df[col] - merged_df[f"{col}_prev"]

        final_cols = ID_COLS + SNAPSHOT_COLS + CUMULATIVE_COLS
        existing_final_cols = [col for col in final_cols if col in merged_df.columns]
        output_df = merged_df[existing_final_cols]
        
        output_path = os.path.join(by_gameweek_path, current_gw_dir, output_filename)
        output_df.to_csv(output_path, index=False)
        logger.info(f"  > Saved calculated stats to '{output_filename}'")

### END NEW SECTION ###

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
        if not isinstance(match_id, str): return None
        for slug in TOURNAMENT_NAME_MAP.keys():
            if slug in match_id:
                return slug
        return None
    matches_df['tournament'] = matches_df['match_id'].apply(extract_tournament_slug)

    # --- Filter out all friendlies and gameweek 0 matches ---
    logger.info("\nFiltering out friendlies and pre-season (GW0) matches...")
    initial_match_count = len(matches_df)
    matches_df = matches_df[(matches_df['gameweek'] != 0) & (matches_df['tournament'] != 'friendly')]
    final_match_count = len(matches_df)
    logger.info(f"  > Removed {initial_match_count - final_match_count} matches. Processing {final_match_count} relevant matches.")

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
            # Added a check to prevent errors if a gw is not in the gameweeks table
            if gw not in gameweeks_df['id'].values: continue
            is_finished = gameweeks_df.loc[gameweeks_df['id'] == gw, 'finished'].iloc[0]
            logger.info(f"  > Processing GW{gw} for {folder_name} (Finished: {is_finished})...")
            
            tournament_gw_path = os.path.join(BASE_DATA_PATH, 'By Tournament', folder_name, f'GW{gw}')
            os.makedirs(tournament_gw_path, exist_ok=True)
            
            gw_tournament_matches = tournament_matches[tournament_matches['gameweek'] == gw]
            match_ids = gw_tournament_matches['match_id'].unique().tolist()
            gw_tournament_playerstats = playermatchstats_df[playermatchstats_df['match_id'].isin(match_ids)]
            
            gw_tournament_matches.to_csv(os.path.join(tournament_gw_path, 'matches.csv'), index=False)
            gw_tournament_playerstats.to_csv(os.path.join(tournament_gw_path, 'playermatchstats.csv'), index=False)

            # Save snapshots for active/current gameweeks
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
        if gw not in gameweeks_df['id'].values: continue
        is_finished = gameweeks_df.loc[gameweeks_df['id'] == gw, 'finished'].iloc[0]
        logger.info(f"Processing GW{gw} (Finished: {is_finished})...")

        gw_path = os.path.join(BASE_DATA_PATH, 'By Gameweek', f'GW{gw}')
        os.makedirs(gw_path, exist_ok=True)
        
        gw_matches = matches_df[matches_df['gameweek'] == gw]
        match_ids = gw_matches['match_id'].unique().tolist()
        gw_playermatchstats = playermatchstats_df[playermatchstats_df['match_id'].isin(match_ids)]
        
        logger.info("  > Saving matches and player-match-stats...")
        gw_matches.to_csv(os.path.join(gw_path, 'matches.csv'), index=False)
        gw_playermatchstats.to_csv(os.path.join(gw_path, 'playermatchstats.csv'), index=False)
        
        # Always save a snapshot of playerstats, players, and teams.
        # This ensures the calculation step has the files it needs.
        logger.info("  > Saving data snapshots...")
        gw_matches.to_csv(os.path.join(gw_path, 'fixtures.csv'), index=False)
        players_df.to_csv(os.path.join(gw_path, 'players.csv'), index=False)
        teams_df.to_csv(os.path.join(gw_path, 'teams.csv'), index=False)
        
        gw_playerstats_snapshot = playerstats_df[playerstats_df['gw'] == gw]
        gw_playerstats_snapshot.to_csv(os.path.join(gw_path, 'playerstats.csv'), index=False)

    ### NEW ###
    # --- 4. Perform the discrete gameweek calculation ---
    calculate_discrete_gameweek_stats()
    ### END NEW ###

    logger.info("\n--- Comprehensive data update process completed successfully! ---")

if __name__ == "__main__":
    main()
