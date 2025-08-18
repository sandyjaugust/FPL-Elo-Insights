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

# --- Setup: Load Environment Variables and Connect to Supabase ---
load_dotenv()
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("FATAL ERROR: SUPABASE_URL and SUPABASE_KEY must be set in your environment or a .env file.")
    exit()

supabase: Client = create_client(url, key)

# --- Helper Functions ---

def create_directory(path: str):
    """Creates a directory if it doesn't already exist."""
    Path(path).mkdir(parents=True, exist_ok=True)

def get_tournament_name_from_id(match_id: str, name_map: dict) -> str:
    """Finds the correct tournament name from a match_id string."""
    for slug, name in sorted(name_map.items(), key=lambda item: len(item[0]), reverse=True):
        if slug in match_id:
            return name
    return "Other"

def fetch_all_records(table_name: str) -> pd.DataFrame:
    """Fetches all records from a master data table."""
    print(f"Fetching all records from master table: '{table_name}'...")
    try:
        response = supabase.table(table_name).select('*', count='exact').execute()
        df = pd.DataFrame(response.data)
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
        if finished_only:
            query = query.eq('finished', True)
        
        response = query.execute()

        if not response.data:
            print(f"  > No gameweeks found in '{table_name}'. Defaulting to 1.")
            return 1
            
        valid_gameweeks = [item[gameweek_col] for item in response.data if item.get(gameweek_col) is not None]
        if not valid_gameweeks:
            print(f"  > No valid gameweek numbers found. Defaulting to 1.")
            return 1
            
        latest_gw = max(valid_gameweeks)
        print(f"  > Latest gameweek found in '{table_name}': {latest_gw}.")
        return latest_gw
    except Exception as e:
        print(f"  ERROR: Could not fetch latest gameweek from '{table_name}': {e}. Defaulting to 1.")
        return 1

def fetch_data_for_gameweek(table_name: str, gameweek: int, gameweek_col: str = 'gameweek') -> pd.DataFrame:
    """Fetches data from a table for a specific gameweek."""
    print(f"Fetching data from '{table_name}' for GW{gameweek}...")
    try:
        response = supabase.table(table_name).select('*').eq(gameweek_col, gameweek).execute()
        df = pd.DataFrame(response.data)
        print(f"  > Fetched {len(df)} rows from '{table_name}'.")
        return df
    except Exception as e:
        print(f"  ERROR fetching from '{table_name}': {e}")
        return pd.DataFrame()

def fetch_data_by_ids(table_name: str, column: str, ids: list) -> pd.DataFrame:
    """Fetches records from a table where a column value is in the provided list of IDs."""
    if not ids: return pd.DataFrame()
    print(f"Fetching {len(ids)} related records from '{table_name}' using '{column}'...")
    all_data = []
    chunk_size = 500  # Supabase has limits on URL length for .in() filter
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
    """
    Updates a CSV file with new data. If the file exists, it merges the data
    and keeps the latest entries based on unique columns. Otherwise, it creates a new file.
    """
    if df.empty:
        # print(f"  > No data to write to {file_path}, skipping.")
        return
        
    create_directory(os.path.dirname(file_path))
    
    if os.path.exists(file_path):
        existing_df = pd.read_csv(file_path)
        combined_df = pd.concat([existing_df, df])
    else:
        combined_df = df
        
    # Drop duplicates, keeping the last entry, which is the newest data
    updated_df = combined_df.drop_duplicates(subset=unique_cols, keep='last')
    updated_df.to_csv(file_path, index=False)
    # print(f"  > Successfully updated {file_path}")

def main():
    """Main function to run the entire data export and processing pipeline."""
    season_path = os.path.join('data', SEASON)
    print(f"--- Starting Automated Data Update for Season {SEASON} ---")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")

    # --- 1. Fetch ALL master data (players, teams, gameweeks) ---
    all_players_df = fetch_all_records('players')
    all_teams_df = fetch_all_records('teams')
    all_gameweeks_df = fetch_all_records('gameweeks')

    # --- 2. Determine the LATEST gameweek to process from the 'matches' and 'playerstats' tables ---
    latest_match_gw = get_latest_gameweek_from_table('matches', finished_only=True)
    latest_playerstats_gw = get_latest_gameweek_from_table('playerstats', gameweek_col='gw', finished_only=False)
    
    # We process the highest gameweek found across both tables to ensure we get all recent data
    start_gameweek = max(latest_match_gw, latest_playerstats_gw)

    # --- 3. Fetch data only for the determined gameweek ---
    matches_df = fetch_data_for_gameweek('matches', start_gameweek)
    player_stats_df = fetch_data_for_gameweek('playerstats', start_gameweek, gameweek_col='gw')

    # Exit early if there's no data for the target gameweek
    if matches_df.empty:
        print(f"\nNo match data found for the latest gameweek (GW{start_gameweek}). No new files will be created.")
        print("\n--- Process complete. ---")
        return

    print("\n--- Pre-processing fetched data ---")
    matches_df['tournament'] = matches_df['match_id'].apply(lambda mid: get_tournament_name_from_id(mid, TOURNAMENT_NAME_MAP))
    
    # Split matches into finished and fixtures
    finished_matches_df = matches_df[matches_df['finished'] == True].copy()
    fixtures_df = matches_df[matches_df['finished'] == False].copy()

    print(f"  > Processing GW{start_gameweek}: Found {len(finished_matches_df)} finished matches and {len(fixtures_df)} upcoming fixtures.")

    # Fetch player-match stats ONLY for the finished matches in our target gameweek
    relevant_match_ids = finished_matches_df['match_id'].unique().tolist()
    player_match_stats_df = fetch_data_by_ids('playermatchstats', 'match_id', relevant_match_ids)

    # Add helper columns to player-match stats
    if not player_match_stats_df.empty:
        match_id_to_tourn_map = matches_df.set_index('match_id')['tournament'].to_dict()
        player_match_stats_df['gameweek'] = start_gameweek  # Assign the current GW
        player_match_stats_df['tournament'] = player_match_stats_df['match_id'].map(match_id_to_tourn_map)

    print("\n--- Saving data into directory structures ---")

    # --- 4. Save data into the 'By Gameweek' structure for the current GW ---
    gw_dir = os.path.join(season_path, "By Gameweek", f"GW{start_gameweek}")
    print(f"\nProcessing 'By Gameweek' directory: {gw_dir}")
    update_csv(finished_matches_df.drop(columns=['tournament'], errors='ignore'), os.path.join(gw_dir, "matches.csv"), unique_cols=['match_id'])
    update_csv(player_match_stats_df.drop(columns=['gameweek', 'tournament'], errors='ignore'), os.path.join(gw_dir, "playermatchstats.csv"), unique_cols=['player_id', 'match_id'])
    update_csv(fixtures_df.drop(columns=['tournament'], errors='ignore'), os.path.join(gw_dir, "fixtures.csv"), unique_cols=['match_id'])
    update_csv(all_players_df, os.path.join(gw_dir, "players.csv"), unique_cols=['player_id'])
    update_csv(all_teams_df, os.path.join(gw_dir, "teams.csv"), unique_cols=['id'])
    update_csv(player_stats_df, os.path.join(gw_dir, "playerstats.csv"), unique_cols=['id', 'gw'])
    
    # --- 5. Save data into the 'By Tournament' structure ---
    print("\nProcessing 'By Tournament' directories...")
    for tourn, group in matches_df.groupby('tournament'):
        tourn_dir = os.path.join(season_path, "By Tournament", tourn, f"GW{start_gameweek}")
        
        tourn_finished_matches = group[group['finished'] == True]
        tourn_fixtures = group[group['finished'] == False]
        tourn_match_ids = tourn_finished_matches['match_id'].unique().tolist()
        tourn_pms = player_match_stats_df[player_match_stats_df['match_id'].isin(tourn_match_ids)]
        
        # Filter the main player_stats_df for players who played in this tournament this gameweek
        tourn_player_ids = tourn_pms['player_id'].unique().tolist()
        tourn_player_stats = player_stats_df[player_stats_df['id'].isin(tourn_player_ids)]

        update_csv(tourn_finished_matches.drop(columns=['tournament'], errors='ignore'), os.path.join(tourn_dir, "matches.csv"), unique_cols=['match_id'])
        update_csv(tourn_pms.drop(columns=['gameweek', 'tournament'], errors='ignore'), os.path.join(tourn_dir, "playermatchstats.csv"), unique_cols=['player_id', 'match_id'])
        update_csv(tourn_fixtures.drop(columns=['tournament'], errors='ignore'), os.path.join(tourn_dir, "fixtures.csv"), unique_cols=['match_id'])
        update_csv(all_players_df, os.path.join(tourn_dir, "players.csv"), unique_cols=['player_id'])
        update_csv(all_teams_df, os.path.join(tourn_dir, "teams.csv"), unique_cols=['id'])
        update_csv(tourn_player_stats, os.path.join(tourn_dir, "playerstats.csv"), unique_cols=['id', 'gw'])
    
    # --- 6. Update Master Files in the root season folder ---
    # These files accumulate all data over the season
    print("\n--- Updating master data files in root directory ---")
    update_csv(all_players_df, os.path.join(season_path, 'players.csv'), unique_cols=['player_id'])
    update_csv(all_teams_df, os.path.join(season_path, 'teams.csv'), unique_cols=['id'])
    update_csv(all_gameweeks_df, os.path.join(season_path, 'gameweek_summaries.csv'), unique_cols=['id'])

    # Append the newly fetched player stats to the master file
    master_player_stats_path = os.path.join(season_path, 'playerstats.csv')
    update_csv(player_stats_df, master_player_stats_path, unique_cols=['id', 'gw'])
    print(f"  > Master files in '{season_path}' updated.")

    print("\n--- Automated data update process completed successfully! ---")

if __name__ == "__main__":
    main()
