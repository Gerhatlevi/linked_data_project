import numpy as np
import pandas as pd

# --- FILE PATHS ---
bo_file = "box_office.csv"
imdb_file = "filtered_title_basics.tsv"
output_file = "boxoffice-with-imdb.csv"

base_path = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\"
output_base_path = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\"

# --- LOAD DATA ---
bo_df = pd.read_csv(base_path + bo_file, encoding="latin-1")
imdb_df = pd.read_csv(base_path + imdb_file, sep="\t", low_memory=False)

# --- CLEAN TITLES ---
# Itt a 'release' oszlop tartalmazza a film címét
bo_df["match_title"] = bo_df["release"].str.lower().str.strip()
imdb_df["match_title"] = imdb_df["primaryTitle"].str.lower().str.strip()
imdb_df["match_title2"] = imdb_df["originalTitle"].str.lower().str.strip()

# --- UNIQUE MOVIES MATCHING ---
# Kigyűjtjük az egyedi filmeket, hogy ne keressünk minden napra újra
unique_movies = bo_df[["match_title"]].drop_duplicates()

print(f"Total rows in BO: {len(bo_df)}, Unique movies to match: {len(unique_movies)}")

# --- MATCHING LOGIC ---
results = []
# Csak a 'movie' típus érdekel minket a Box Office-nál
movie_types = ["movie"]

for _, row in unique_movies.iterrows():
    candidates = imdb_df[
        ((imdb_df["match_title"] == row["match_title"]) | 
         (imdb_df["match_title2"] == row["match_title"])) &
        (imdb_df["titleType"].isin(movie_types))
    ]
    
    if not candidates.empty:
        # Ha több találat van, a legújabbra tippelünk (a Box Office adatok általában újak)
        best_tconst = candidates.sort_values("startYear", ascending=False).iloc[0]["tconst"]
        results.append({"match_title": row["match_title"], "imdb_tconst": best_tconst})
    else:
        results.append({"match_title": row["match_title"], "imdb_tconst": None})

match_lookup = pd.DataFrame(results)

# --- MERGE BACK ---
bo_df = pd.merge(bo_df, match_lookup, on="match_title", how="left")

# --- FINAL CLEANUP ---
# Eredeti sorrend megtartása (Dátum és napi helyezés szerint)
bo_df = bo_df.sort_values(by=["date", "td"], ascending=[True, True])

# Segédoszlop törlése
bo_df = bo_df.drop(columns=["match_title"])
bo_df = bo_df[bo_df["imdb_tconst"].notna()]

# --- SAVE ---
bo_df.to_csv(output_base_path + output_file, index=False)

print(f"Done! Matched {match_lookup['imdb_tconst'].notna().sum()} movies out of {len(unique_movies)}")