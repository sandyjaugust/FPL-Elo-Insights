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
    """Fetches records from a table where a column value is in the provided list of IDs."""
    if not ids: return pd.DataFrame()
    all_data = []
    chunk_size = 500
    for i in range(0, len(ids), chunk_size):
        chunk_ids = ids[i:i + chunk_size]
        try:
            response = supabase.table(table_name).select('*').in_(column, chunk_ids).execute()
            all_data.extend(response.data)
        except Exception as e:
            print(f"  ERROR fetching chunk from '{table_name}': {e}")
    df = pd.DataFrame(all_data)
    print(f"  > Fetched {len(df)} total rows from '{table_name}'.")
    return df

def update_csv(df: pd.DataFrame, file_path: str, unique_cols: list):
    """Updates a CSV by merging new data and removing duplicates, keeping the latest."""
    if df.empty: return
    create_directory(os.path.dirname(file_path))
    if os.path.exists(file_path):
        existing_df = pd.read_csv(file_path)
        combined_df = pd.concat([existing_df, df])
    else:
        combined_df = df
    updated_df = combined_df.drop_duplicates(subset=unique_cols, keep='last')
    updated_df.to_csv(file_path, index=False)


def main():
    season_path = os.path.join('data', SEASON)
    print(f"--- Starting Automated Data Update for Season {SEASON} ---")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")

    # This variable will hold the dataframe from the local summary file
    local_gws_df = pd.DataFrame()
    gameweek_summaries_path = os.path.join(season_path, 'gameweek_summaries.csv')
    if os.path.exists(gameweek_summaries_path):
        print("Local 'gameweek_summaries.csv' found. Determining start gameweek from local file.")
        try:
            local_gws_df = pd.read_csv(gameweek_summaries_path)
            finished_gws_df = local_gws_df[local_gws_df['finished'] == True]

            if not finished_gws_df.empty:
                latest_finished_gw = int(finished_gws_df['id'].max())
                start_gameweek = latest_finished_gw
                print(f"  > Latest FINISHED gameweek in local file: {latest_finished_gw}. Processing from there.")
            else:
                print("  > No finished gameweeks found in local file. Starting from GW1.")
                start_gameweek = 1
        except Exception as e:
            print(f"  ERROR: Could not read local file: {e}. Falling back to database query.")
            start_gameweek = get_latest_gameweek_from_table('matches', finished_only=True)
    else:
        print("Local 'gameweek_summaries.csv' not found. Performing initial data pull from database.")
        start_gameweek = get_latest_gameweek_from_table('matches', finished_only=True)

    all_players_df = fetch_all_records('players')
    all_teams_df = fetch_all_records('teams')
    all_gameweeks_df = fetch_all_records('gameweeks')

    print(f"\n--- Processing data for Gameweek {start_gameweek} and onwards ---")

    matches_df = fetch_data_since_gameweek('matches', start_gameweek)
    recent_player_stats_df = fetch_data_since_gameweek('playerstats', start_gameweek, gameweek_col='gw')

    if matches_df.empty:
        print("\nNo new/upcoming match data to process. Updating master files only.")
        update_csv(all_players_df, os.path.join(season_path, 'players.csv'), unique_cols=['player_id'])
        update_csv(all_teams_df, os.path.join(season_path, 'teams.csv'), unique_cols=['id'])
        update_csv(all_gameweeks_df, os.path.join(season_path, 'gameweek_summaries.csv'), unique_cols=['id'])
        update_csv(recent_player_stats_df, os.path.join(season_path, 'playerstats.csv'), unique_cols=['id', 'gw'])
        print("\n--- Process complete. ---")
        return

    print("\n--- Pre-processing fetched data ---")
    matches_df['tournament'] = matches_df['match_id'].apply(lambda mid: get_tournament_name_from_id(mid, TOURNAMENT_NAME_MAP))

    all_gws = sorted(matches_df['gameweek'].dropna().unique())
    print(f"Found data for new gameweeks: {all_gws}")

    # ⭐️ NEW LOGIC: Get a set of finished GW IDs for quick lookups inside the loop
    finished_gw_ids = set()
    if not local_gws_df.empty:
        finished_gw_ids = set(local_gws_df[local_gws_df['finished'] == True]['id'])

    for gw in all_gws:
        gw = int(gw)
        print(f"\n--- Saving data for GW{gw} ---")

        # ⭐️ NEW LOGIC: Check if the current gameweek is already finished
        gw_is_finished = gw in finished_gw_ids

        gw_matches_df = matches_df[matches_df['gameweek'] == gw]
        gw_finished_matches_df = gw_matches_df[gw_matches_df['finished'] == True].copy()
        gw_fixtures_df = gw_matches_df[gw_matches_df['finished'] == False].copy()

        gw_player_stats_df = pd.DataFrame()
        if not recent_player_stats_df.empty:
            gw_player_stats_df = recent_player_stats_df[recent_player_stats_df['gw'] == gw].copy()

        relevant_match_ids = gw_finished_matches_df['match_id'].unique().tolist()
        player_match_stats_df = fetch_data_by_ids('playermatchstats', 'match_id', relevant_match_ids)

        gw_dir = os.path.join(season_path, "By Gameweek", f"GW{gw}")
        update_csv(gw_finished_matches_df, os.path.join(gw_dir, "matches.csv"), unique_cols=['match_id'])
        update_csv(gw_fixtures_df, os.path.join(gw_dir, "fixtures.csv"), unique_cols=['match_id'])
        update_csv(player_match_stats_df, os.path.join(gw_dir, "playermatchstats.csv"), unique_cols=['player_id', 'match_id'])
        update_csv(gw_player_stats_df, os.path.join(gw_dir, "playerstats.csv"), unique_cols=['id', 'gw'])

        # ⭐️ NEW LOGIC: Only update player/team snapshots if the gameweek is NOT finished
        if not gw_is_finished:
            print("  > Gameweek is not finished. Fetching and updating player/team snapshots.")
            relevant_team_codes = pd.concat([gw_matches_df['home_team'], gw_matches_df['away_team']]).dropna().unique().tolist()
            relevant_players_df = fetch_data_by_ids('players', 'team_code', relevant_team_codes)
            relevant_teams_df = fetch_data_by_ids('teams', 'id', relevant_team_codes)

            update_csv(relevant_players_df, os.path.join(gw_dir, "players.csv"), unique_cols=['player_id'])
            update_csv(relevant_teams_df, os.path.join(gw_dir, "teams.csv"), unique_cols=['id'])
        else:
            print("  > Gameweek is finished. Skipping player/team snapshot update to preserve history.")
        
        print(f"  > Saved data to '{gw_dir}'")

        # The tournament-specific folders are built from the gameweek data,
        # so they will also respect the historical snapshot logic implicitly.
        # No changes needed in the tournament loop below.
        for tourn, group in gw_matches_df.groupby('tournament'):
            tourn_dir = os.path.join(season_path, "By Tournament", tourn, f"GW{gw}")
            update_csv(group[group['finished'] == True], os.path.join(tourn_dir, "matches.csv"), unique_cols=['match_id'])
            update_csv(group[group['finished'] == False], os.path.join(tourn_dir, "fixtures.csv"), unique_cols=['match_id'])
            # ... and so on for other tournament files if needed ...
            print(f"  > Saved data to '{tourn_dir}'")

    print("\n--- Updating master data files in root directory ---")
    update_csv(all_players_df, os.path.join(season_path, 'players.csv'), unique_cols=['player_id'])
    update_csv(all_teams_df, os.path.join(season_path, 'teams.csv'), unique_cols=['id'])
    update_csv(all_gameweeks_df, os.path.join(season_path, 'gameweek_summaries.csv'), unique_cols=['id'])

    finished_gameweeks_in_run = [gw for gw in all_gws if gw <= start_gameweek]
    if finished_gameweeks_in_run and not recent_player_stats_df.empty:
        print(f"  > Updating master 'playerstats.csv' with data for finished GWs: {finished_gameweeks_in_run}")
        finished_player_stats_df = recent_player_stats_df[recent_player_stats_df['gw'].isin(finished_gameweeks_in_run)]
        update_csv(finished_player_stats_df, os.path.join(season_path, 'playerstats.csv'), unique_cols=['id', 'gw'])
    else:
        print("  > No new finished gameweek playerstats to update in master file.")

    print(f"  > Master files in '{season_path}' updated.")
    print("\n--- Automated data update process completed successfully! ---")

if __name__ == "__main__":
    main()
