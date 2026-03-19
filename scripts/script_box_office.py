import numpy as np
import pandas as pd

# --- FILE PATHS ---
bo_file = "box_office.csv"
imdb_file = "filtered_title_basics.tsv"
output_file = "boxoffice-with-imdb.csv"

base_path = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\"
output_base_path = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\used\\"

# --- LOAD DATA ---
bo_df = pd.read_csv(base_path + bo_file, encoding="latin-1")
imdb_df = pd.read_csv(output_base_path + imdb_file, sep="\t", low_memory=False)

# --- CLEAN TITLES ---
bo_df["match_title"] = bo_df["release"].str.lower().str.strip()
imdb_df["match_title"] = imdb_df["primaryTitle"].str.lower().str.strip()
imdb_df["match_title2"] = imdb_df["originalTitle"].str.lower().str.strip()

# --- UNIQUE MOVIES MATCHING ---
unique_movies = bo_df[["match_title"]].drop_duplicates()

# --- MATCHING LOGIC ---
results = []
movie_types = ["movie"]

for _, row in unique_movies.iterrows():
    candidates = imdb_df[
        ((imdb_df["match_title"] == row["match_title"]) | 
         (imdb_df["match_title2"] == row["match_title"])) &
        (imdb_df["titleType"].isin(movie_types))
    ]
    
    if not candidates.empty:
        best_tconst = candidates.sort_values("startYear", ascending=False).iloc[0]["tconst"]
        results.append({"match_title": row["match_title"], "imdb_tconst": best_tconst})
    else:
        results.append({"match_title": row["match_title"], "imdb_tconst": None})

match_lookup = pd.DataFrame(results)

# --- MERGE BACK ---
bo_df = pd.merge(bo_df, match_lookup, on="match_title", how="left")

# --- FINAL CLEANUP ---
bo_df = bo_df.sort_values(by=["date", "td"], ascending=[True, True])

bo_df = bo_df.drop(columns=["match_title", "release"])
bo_df = bo_df[bo_df["imdb_tconst"].notna()]

# --- SAVE ---
bo_df.to_csv(output_base_path + output_file, index=False)

print(f"Done! Matched {match_lookup['imdb_tconst'].notna().sum()} movies out of {len(unique_movies)}")