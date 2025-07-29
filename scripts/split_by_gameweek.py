import pandas as pd
from pathlib import Path
import sys

def main():
    # File paths
    matches_file = Path("data/2024-2025/matches/matches.csv")
    playerstats_file = Path("data/2024-2025/playermatchstats/playermatchstats.csv")
    
    print("🚀 Starting CSV split by gameweek...")
    
    # Check if files exist
    if not matches_file.exists():
        print(f"❌ Error: Matches file not found: {matches_file}")
        sys.exit(1)
    
    if not playerstats_file.exists():
        print(f"❌ Error: Player stats file not found: {playerstats_file}")
        sys.exit(1)
    
    print(f"✅ Found matches file: {matches_file}")
    print(f"✅ Found player stats file: {playerstats_file}")
    
    try:
        # Read the matches CSV
        print("\n📖 Reading matches CSV...")
        matches_df = pd.read_csv(matches_file)
        print(f"   Total matches: {len(matches_df)}")
        
        # Get unique gameweeks
        gameweeks = sorted(matches_df['gameweek'].unique())
        print(f"   Found gameweeks: {gameweeks}")
        
        # Create match_id to gameweek mapping
        match_gameweek_map = dict(zip(matches_df['match_id'], matches_df['gameweek']))
        
        # Split matches by gameweek
        print("\n🔄 Splitting matches by gameweek...")
        matches_dir = matches_file.parent
        
        for gw in gameweeks:
            # Create GW folder
            gw_folder = matches_dir / f"GW{gw}"
            gw_folder.mkdir(exist_ok=True)
            
            # Filter matches for this gameweek
            gw_matches = matches_df[matches_df['gameweek'] == gw]
            
            # Save matches for this gameweek
            output_file = gw_folder / "matches.csv"
            gw_matches.to_csv(output_file, index=False)
            print(f"   📁 GW{gw}: {len(gw_matches)} matches → {output_file}")
        
        # Read and split player stats
        print("\n📖 Reading player stats CSV...")
        playerstats_df = pd.read_csv(playerstats_file)
        print(f"   Total player stat records: {len(playerstats_df)}")
        
        # Add gameweek column to player stats using match_id mapping
        playerstats_df['gameweek'] = playerstats_df['match_id'].map(match_gameweek_map)
        
        # Check for unmapped records
        unmapped_count = playerstats_df['gameweek'].isna().sum()
        if unmapped_count > 0:
            print(f"   ⚠️  Warning: {unmapped_count} records couldn't be mapped to gameweek")
            playerstats_df = playerstats_df.dropna(subset=['gameweek'])
        
        # Split player stats by gameweek
        print("\n🔄 Splitting player stats by gameweek...")
        playerstats_dir = playerstats_file.parent
        
        for gw in gameweeks:
            # Create GW folder
            gw_folder = playerstats_dir / f"GW{gw}"
            gw_folder.mkdir(exist_ok=True)
            
            # Filter player stats for this gameweek
            gw_stats = playerstats_df[playerstats_df['gameweek'] == gw]
            
            if len(gw_stats) > 0:
                # Remove the temporary gameweek column before saving
                gw_stats_clean = gw_stats.drop('gameweek', axis=1)
                
                # Save player stats for this gameweek
                output_file = gw_folder / "playermatchstats.csv"
                gw_stats_clean.to_csv(output_file, index=False)
                print(f"   📁 GW{gw}: {len(gw_stats)} player records → {output_file}")
        
        print("\n✅ Split completed successfully!")
        print("\n📊 Summary:")
        print(f"   Created folders for {len(gameweeks)} gameweeks")
        print(f"   Matches directory: {matches_dir}")
        print(f"   Player stats directory: {playerstats_dir}")
        
        # Show final structure
        print("\n📂 Created structure:")
        for gw in gameweeks:
            matches_gw_file = matches_dir / f"GW{gw}" / "matches.csv"
            stats_gw_file = playerstats_dir / f"GW{gw}" / "playermatchstats.csv"
            
            print(f"   GW{gw}/")
            if matches_gw_file.exists():
                match_count = len(pd.read_csv(matches_gw_file))
                print(f"     matches/{matches_gw_file.name} ({match_count} matches)")
            if stats_gw_file.exists():
                stats_count = len(pd.read_csv(stats_gw_file))
                print(f"     playermatchstats/{stats_gw_file.name} ({stats_count} records)")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
