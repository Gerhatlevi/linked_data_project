import numpy as np
import pandas as pd

# --- FILE PATHS ---
netflix_file = "most-popular.csv"
imdb_file = "filtered_title_basics.tsv"
output_file = "most_popular_with_imdb.csv"
base_path = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\"
output_base_path = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\"

# --- LOAD DATA ---
netflix_df = pd.read_csv(base_path + netflix_file)
imdb_df = pd.read_csv(base_path + imdb_file, sep="\t")

# --- CLEAN TITLES (lowercase for matching) ---
netflix_df["match_title"] = netflix_df["show_title"].str.lower().str.strip()
imdb_df["match_title"] = imdb_df["primaryTitle"].str.lower().str.strip()
imdb_df["match_title2"] = imdb_df["originalTitle"].str.lower().str.strip()

# --- REMOVE DUPLICATES IN IMDB (keep first match) ---
#imdb_unique = imdb_df.drop_duplicates(subset=["match_title"])

def get_expected_type(category):
    if isinstance(category, str) and "film" in category.lower():
        return ["movie"]
    elif isinstance(category, str) and "tv" in category.lower():
        return ["tvSeries", "tvMiniSeries"]
    else:
        return []

netflix_df["expected_type"] = netflix_df["category"].apply(get_expected_type)
netflix_df["runtime_minutes"] = netflix_df["runtime"].astype(float) * 60
imdb_df["runtimeMinutes"] = pd.to_numeric(imdb_df["runtimeMinutes"], errors="coerce")

def find_best_match(row):
    candidates = imdb_df[
        ((imdb_df["match_title"] == row["match_title"]) | (imdb_df["match_title2"] == row["match_title"])) &
        (imdb_df["titleType"].isin(row["expected_type"]))
    ]
    
    if candidates.empty:
        return None
    
    # If runtime available, use closest runtime
    if not np.isnan(row["runtime_minutes"]):
        candidates["runtime_diff"] = abs(
            candidates["runtimeMinutes"] - row["runtime_minutes"]
        )
        best = candidates.sort_values("runtime_diff").iloc[0]
    else:
        best = candidates.iloc[0]
    
    return best["tconst"]

netflix_df["imdb_tconst"] = netflix_df.apply(find_best_match, axis=1)
netflix_df = netflix_df[netflix_df["imdb_tconst"].notna()]

netflix_df = netflix_df.drop(columns=["match_title", "expected_type", "runtime_minutes"])

# --- Save ---
netflix_df.to_csv(output_base_path + output_file, index=False)

print("Done! File saved as:", output_file)