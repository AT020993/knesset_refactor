import duckdb
import pandas as pd
from pathlib import Path
import sys

# --- Configuration ---
# Determine project root: Assuming this script is in a 'scripts' directory
# If script is elsewhere, this might need adjustment.
try:
    SCRIPT_DIR = Path(__file__).resolve().parent
    PROJECT_ROOT = SCRIPT_DIR.parent
except NameError: # Fallback for interactive use (e.g. Jupyter notebook)
    PROJECT_ROOT = Path.cwd()
    # If running interactively and not from scripts dir, ensure PROJECT_ROOT is correct
    if not (PROJECT_ROOT / "data" / "parquet").exists():
        # Try one level up if current dir is 'scripts'
        if (PROJECT_ROOT.parent / "data" / "parquet").exists() and PROJECT_ROOT.name == "scripts":
            PROJECT_ROOT = PROJECT_ROOT.parent
        else: # Heuristic failed
            print("Error: Could not reliably determine project root for interactive use.", file=sys.stderr)
            print(f"Current working directory: {PROJECT_ROOT}", file=sys.stderr)
            print("Please ensure data/parquet and data/ are accessible relative to this path, or adjust PROJECT_ROOT.", file=sys.stderr)


PARQUET_DIR = PROJECT_ROOT / "data" / "parquet"
# Changed: Output CSV will now be directly in the 'data' folder
DATA_DIR_FOR_OUTPUT_CSV = PROJECT_ROOT / "data" 
OUTPUT_CSV_FILENAME = "faction_coalition_status.csv"
OUTPUT_CSV_PATH = DATA_DIR_FOR_OUTPUT_CSV / OUTPUT_CSV_FILENAME

# Define the expected columns for the final CSV
# This order will be enforced in the output.
EXPECTED_COLUMNS = [
    'KnessetNum', 'FactionID', 'FactionName',
    'FactionStartDate', 'FactionFinishDate', 'CoalitionStatus'
]

def get_all_factions_from_parquet() -> pd.DataFrame:
    """
    Retrieves all factions from all Knessets from the local Parquet files.

    Returns:
        A pandas DataFrame with columns: KnessetNum, FactionID, FactionName,
        FactionStartDate, FactionFinishDate. Returns an empty DataFrame on error.
    """
    kns_faction_parquet = PARQUET_DIR / "KNS_Faction.parquet"
    kns_knesset_parquet = PARQUET_DIR / "KNS_Knesset.parquet"

    if not kns_faction_parquet.exists() or not kns_knesset_parquet.exists():
        print(f"Error: Parquet files not found in {PARQUET_DIR}.", file=sys.stderr)
        print("Please ensure KNS_Faction.parquet and KNS_Knesset.parquet are downloaded.", file=sys.stderr)
        return pd.DataFrame(columns=EXPECTED_COLUMNS[:-1]) # Return empty DF with expected API columns

    try:
        con = duckdb.connect(database=':memory:', read_only=True)
        query = f"""
        SELECT
            k.KnessetNum,
            f.FactionID,
            f.Name AS FactionName,
            f.StartDate AS FactionStartDate,
            f.FinishDate AS FactionFinishDate
        FROM
            read_parquet('{kns_faction_parquet.as_posix()}') AS f
        JOIN
            read_parquet('{kns_knesset_parquet.as_posix()}') AS k ON f.KnessetID = k.KnessetID
        ORDER BY
            k.KnessetNum, f.StartDate, f.Name;
        """
        api_df = con.execute(query).fetchdf()
        con.close()

        # Ensure correct data types, especially for dates if they aren't already
        api_df['FactionStartDate'] = pd.to_datetime(api_df['FactionStartDate'])
        api_df['FactionFinishDate'] = pd.to_datetime(api_df['FactionFinishDate'], errors='coerce') # Handles NaT for null finish dates
        
        # Cast IDs to int
        api_df['KnessetNum'] = api_df['KnessetNum'].astype(int)
        api_df['FactionID'] = api_df['FactionID'].astype(int)

        return api_df

    except Exception as e:
        print(f"Error querying Parquet files: {e}", file=sys.stderr)
        return pd.DataFrame(columns=EXPECTED_COLUMNS[:-1])


def main():
    """
    Main function to fetch faction data, merge with coalition status from the CSV,
    and save back to the CSV.
    """
    print(f"Project root detected as: {PROJECT_ROOT}")
    print(f"Parquet directory: {PARQUET_DIR}")
    print(f"Output CSV path: {OUTPUT_CSV_PATH}") # Updated print statement

    if not PARQUET_DIR.exists():
        print(f"Error: Parquet directory not found at {PARQUET_DIR}", file=sys.stderr)
        print("Please ensure your data fetching scripts have run successfully.", file=sys.stderr)
        return

    api_factions_df = get_all_factions_from_parquet()

    if api_factions_df.empty and not (PARQUET_DIR / "KNS_Faction.parquet").exists():
        return
    elif api_factions_df.empty:
        print("No faction data retrieved from Parquet files. CSV will not be updated/created.", file=sys.stderr)
        return

    print(f"Fetched {len(api_factions_df)} faction records from Parquet files.")

    # Prepare the directory for the output CSV
    DATA_DIR_FOR_OUTPUT_CSV.mkdir(parents=True, exist_ok=True) # Ensures 'data' directory exists

    if OUTPUT_CSV_PATH.exists(): # Check for the CSV at the new path
        print(f"Loading existing data from: {OUTPUT_CSV_PATH}") # Updated print statement
        try:
            existing_csv_df = pd.read_csv(
                OUTPUT_CSV_PATH, # Load from the new path
                dtype={
                    'FactionID': int,
                    'KnessetNum': int,
                    'CoalitionStatus': str
                },
                parse_dates=['FactionStartDate', 'FactionFinishDate']
            )
            
            # Select only relevant columns to avoid issues if CSV has extra cols
            existing_csv_df = existing_csv_df[['FactionID', 'CoalitionStatus']]
            
            print(f"Loaded {len(existing_csv_df)} records from {OUTPUT_CSV_FILENAME}.")

            # Merge API data with existing CoalitionStatus
            merged_df = pd.merge(api_factions_df, existing_csv_df, on='FactionID', how='left')
            
            merged_df['CoalitionStatus'] = merged_df['CoalitionStatus'].fillna('')
            print("Merged API data with existing coalition statuses.")

        except Exception as e:
            print(f"Error loading or processing existing {OUTPUT_CSV_PATH}: {e}", file=sys.stderr)
            print("Proceeding with API data only and creating a new CoalitionStatus column.", file=sys.stderr)
            merged_df = api_factions_df.copy()
            merged_df['CoalitionStatus'] = ''
    else:
        print(f"File {OUTPUT_CSV_PATH} not found. Creating new file with empty CoalitionStatus.") # Updated print
        merged_df = api_factions_df.copy()
        merged_df['CoalitionStatus'] = ''

    for col in EXPECTED_COLUMNS:
        if col not in merged_df.columns:
            if col == 'CoalitionStatus':
                 merged_df[col] = ''
            else:
                 merged_df[col] = pd.NA

    final_df = merged_df[EXPECTED_COLUMNS]
    
    final_df = final_df.sort_values(by=['KnessetNum', 'FactionStartDate', 'FactionName']).reset_index(drop=True)

    try:
        final_df['FactionStartDate'] = final_df['FactionStartDate'].dt.strftime('%Y-%m-%d')
        final_df['FactionFinishDate'] = final_df['FactionFinishDate'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else '')

        final_df.to_csv(OUTPUT_CSV_PATH, index=False, encoding='utf-8') # Save to the new path
        print(f"Successfully updated/created {OUTPUT_CSV_PATH} with {len(final_df)} records.") # Updated print
    except Exception as e:
        print(f"Error writing to CSV {OUTPUT_CSV_PATH}: {e}", file=sys.stderr) # Updated print

if __name__ == "__main__":
    main()