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

# --- NEW EFFICIENT FUNCTION ---
def fetch_data_since_gameweek(table_name: str, start_gameweek: int, gameweek_col: str = 'gameweek') -> pd.DataFrame:
    """Fetches data from a table from a specific gameweek onwards."""
    print(f"Fetching data from '{table_name}' for GW{start_gameweek} onwards (Incremental Load)...")
    try:
        # Use gte (greater than or equal to) for an efficient incremental fetch
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

    # --- 1. Fetch ALL master data (players, teams, etc. are small tables). ---
    all_players_df = fetch_all_records('players')
    all_teams_df = fetch_all_records('teams')
    all_gameweeks_df = fetch_all_records('gameweeks')

    # --- 2. Determine the latest gameweek to process from. ---
    start_gameweek = get_latest_gameweek_from_table('matches', finished_only=True)
    print(f"\n--- Processing data for Gameweek {start_gameweek} and onwards ---")

    # --- 3. EFFICIENTLY fetch only RECENT matches and playerstats. ---
    matches_df = fetch_data_since_gameweek('matches', start_gameweek)
    # INCREMENTAL LOAD: Only get playerstats for the new/current gameweeks.
    recent_player_stats_df = fetch_data_since_gameweek('playerstats', start_gameweek, gameweek_col='gw')

    if matches_df.empty:
        print("\nNo new/upcoming match data to process. Updating master files only.")
        update_csv(all_players_df, os.path.join(season_path, 'players.csv'), unique_cols=['player_id'])
        update_csv(all_teams_df, os.path.join(season_path, 'teams.csv'), unique_cols=['id'])
        update_csv(all_gameweeks_df, os.path.join(season_path, 'gameweek_summaries.csv'), unique_cols=['id'])
        # Also update the master playerstats file with any recent stats, even if no matches
        update_csv(recent_player_stats_df, os.path.join(season_path, 'playerstats.csv'), unique_cols=['id', 'gw'])
        print("\n--- Process complete. ---")
        return

    print("\n--- Pre-processing fetched data ---")
    matches_df['tournament'] = matches_df['match_id'].apply(lambda mid: get_tournament_name_from_id(mid, TOURNAMENT_NAME_MAP))
    finished_matches_df = matches_df[matches_df['finished'] == True].copy()
    
    # --- 4. Process each new gameweek found in the fetched data ---
    all_gws = sorted(matches_df['gameweek'].dropna().unique())
    print(f"Found data for new gameweeks: {all_gws}")
    
    for gw in all_gws:
        gw = int(gw)
        print(f"\n--- Saving data for GW{gw} ---")
        
        # Filter dataframes for the specific gameweek (gw)
        gw_matches_df = matches_df[matches_df['gameweek'] == gw]
        gw_finished_matches_df = gw_matches_df[gw_matches_df['finished'] == True].copy()
        gw_fixtures_df = gw_matches_df[gw_matches_df['finished'] == False].copy()
        gw_player_stats_df = recent_player_stats_df[recent_player_stats_df['gw'] == gw].copy()

        relevant_match_ids = gw_finished_matches_df['match_id'].unique().tolist()
        player_match_stats_df = fetch_data_by_ids('playermatchstats', 'match_id', relevant_match_ids)

        # Save to "By Gameweek" structure
        gw_dir = os.path.join(season_path, "By Gameweek", f"GW{gw}")
        update_csv(gw_finished_matches_df, os.path.join(gw_dir, "matches.csv"), unique_cols=['match_id'])
        update_csv(gw_fixtures_df, os.path.join(gw_dir, "fixtures.csv"), unique_cols=['match_id'])
        update_csv(player_match_stats_df, os.path.join(gw_dir, "playermatchstats.csv"), unique_cols=['player_id', 'match_id'])
        update_csv(all_players_df, os.path.join(gw_dir, "players.csv"), unique_cols=['player_id'])
        update_csv(all_teams_df, os.path.join(gw_dir, "teams.csv"), unique_cols=['id'])
        update_csv(gw_player_stats_df, os.path.join(gw_dir, "playerstats.csv"), unique_cols=['id', 'gw'])
        print(f"  > Saved data to '{gw_dir}'")
        
        # Save to "By Tournament" structure
        for tourn, group in gw_matches_df.groupby('tournament'):
            tourn_dir = os.path.join(season_path, "By Tournament", tourn, f"GW{gw}")
            
            tourn_finished_matches = group[group['finished'] == True]
            tourn_fixtures = group[group['finished'] == False]
            tourn_match_ids = tourn_finished_matches['match_id'].unique().tolist()
            tourn_pms = player_match_stats_df[player_match_stats_df['match_id'].isin(tourn_match_ids)]
            
            tourn_home_teams = group['home']
            tourn_away_teams = group['away']
            tourn_team_ids = pd.concat([tourn_home_teams, tourn_away_teams]).unique().tolist()
            
            players_in_tourn_teams = all_players_df[all_players_df['team_id'].isin(tourn_team_ids)]['player_id'].unique().tolist()
            tourn_player_stats = gw_player_stats_df[gw_player_stats_df['id'].isin(players_in_tourn_teams)]

            update_csv(tourn_finished_matches, os.path.join(tourn_dir, "matches.csv"), unique_cols=['match_id'])
            update_csv(tourn_fixtures, os.path.join(tourn_dir, "fixtures.csv"), unique_cols=['match_id'])
            update_csv(tourn_pms, os.path.join(tourn_dir, "playermatchstats.csv"), unique_cols=['player_id', 'match_id'])
            update_csv(all_players_df, os.path.join(tourn_dir, "players.csv"), unique_cols=['player_id'])
            update_csv(all_teams_df, os.path.join(tourn_dir, "teams.csv"), unique_cols=['id'])
            update_csv(tourn_player_stats, os.path.join(tourn_dir, "playerstats.csv"), unique_cols=['id', 'gw'])
            print(f"  > Saved data to '{tourn_dir}'")

    # --- 5. Update Master Files in the root season folder ---
    print("\n--- Updating master data files in root directory ---")
    update_csv(all_players_df, os.path.join(season_path, 'players.csv'), unique_cols=['player_id'])
    update_csv(all_teams_df, os.path.join(season_path, 'teams.csv'), unique_cols=['id'])
    update_csv(all_gameweeks_df, os.path.join(season_path, 'gameweek_summaries.csv'), unique_cols=['id'])
    # The magic of update_csv: It reads the existing master file and merges the new data in.
    update_csv(recent_player_stats_df, os.path.join(season_path, 'playerstats.csv'), unique_cols=['id', 'gw'])
    print(f"  > Master files in '{season_path}' updated.")

    print("\n--- Automated data update process completed successfully! ---")

if __name__ == "__main__":
    main()
