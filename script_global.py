import numpy as np
import pandas as pd
import re

# --- FILE PATHS ---
netflix_file = "all-weeks-global.csv"
imdb_file = "filtered_title_basics.tsv"
output_file = "global-with-imdb.csv"



# --- LOAD DATA ---
netflix_df = pd.read_csv(netflix_file, encoding="latin-1")
imdb_df = pd.read_csv(imdb_file, sep="\t")

# Convert season titles to season numbers
for i, row in netflix_df.iterrows():
    # Iterate through rows with a season title
    if str(row['season_title']) != 'nan':
        # Get all the numbers in a title (last one should be the season number
        numbers_in_title = re.findall("[0-9]+", row['season_title'])
        # If there are no numbers or the last number is higher than 40 (and so probably not a season number,
        # longest ever show is 37 seasons), assume it is season 1 (single season show)
        if not numbers_in_title or int(numbers_in_title[-1]) > 40:
            season = 1
        else:
            season = int(numbers_in_title[-1])
        netflix_df.at[i, 'season_title'] = season

# --- DROP UNUSED COLUMN ---
if "episode_launch_details" in netflix_df.columns:
    netflix_df = netflix_df.drop(columns=["episode_launch_details"])

# --- CLEAN TITLES ---
netflix_df["match_title"] = netflix_df["show_title"].str.lower().str.strip()
imdb_df["match_title"] = imdb_df["primaryTitle"].str.lower().str.strip()
imdb_df["match_title2"] = imdb_df["originalTitle"].str.lower().str.strip()

# --- TYPE MAPPING ---
def get_expected_types(category):
    if isinstance(category, str) and "film" in category.lower():
        return ["movie"]
    elif isinstance(category, str) and "tv" in category.lower():
        return ["tvSeries", "tvMiniSeries"]
    else:
        return []

netflix_df["expected_types"] = netflix_df["category"].apply(get_expected_types)

# --- RUNTIME CONVERSION ---
netflix_df["runtime_minutes"] = pd.to_numeric(netflix_df["runtime"], errors="coerce") * 60

# --- CLEAN IMDB RUNTIME ---
imdb_df["runtimeMinutes"] = pd.to_numeric(imdb_df["runtimeMinutes"], errors="coerce")

# --- MATCH FUNCTION ---
def find_best_match(row):
    candidates = imdb_df[
        (
            (imdb_df["match_title"] == row["match_title"]) |
            (imdb_df["match_title2"] == row["match_title"])
        ) &
        (imdb_df["titleType"].isin(row["expected_types"]))
    ]
    
    if candidates.empty:
        return None
    
    # runtime-based disambiguation
    if not np.isnan(row["runtime_minutes"]):
        candidates = candidates.copy()
        candidates["runtime_diff"] = abs(
            candidates["runtimeMinutes"] - row["runtime_minutes"]
        )
        best = candidates.sort_values("runtime_diff").iloc[0]
    else:
        best = candidates.iloc[0]
    
    return best["tconst"]

# --- APPLY MATCHING ---
netflix_df["imdb_tconst"] = netflix_df.apply(find_best_match, axis=1)
netflix_df = netflix_df[netflix_df["imdb_tconst"].notna()]

# --- CLEANUP ---
netflix_df = netflix_df.drop(columns=["match_title", "expected_types", "runtime_minutes"])

# --- SAVE ---
netflix_df.to_csv(output_file, index=False)

print("Done! File saved as:", output_file)