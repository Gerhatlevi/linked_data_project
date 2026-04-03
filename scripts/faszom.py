import numpy as np
import pandas as pd

# --- FILE PATHS ---
bo_file = "box_office.csv"
imdb_file = "filtered_title_basics.tsv"
ratings_file = "filtered_title_ratings.tsv"
output_file = "boxoffice-with-imdb2.csv"

base_path = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\"
output_base_path = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\used\\"

# --- LOAD DATA ---
bo_df = pd.read_csv(base_path + bo_file, encoding="latin-1")
imdb_df = pd.read_csv(output_base_path + imdb_file, sep="\t", low_memory=False)
ratings_df = pd.read_csv(output_base_path + ratings_file, sep="\t", low_memory=False)

# --- CLEAN TITLES ---
bo_df["match_title"] = bo_df["release"].str.lower().str.strip()
imdb_df["startYear"] = pd.to_numeric(imdb_df["startYear"], errors="coerce")
imdb_df = imdb_df[imdb_df["titleType"] == "movie"].copy()

# --- MERGE RATINGS INTO IMDB ---
imdb_df = imdb_df.merge(ratings_df[["tconst", "numVotes"]], on="tconst", how="left")
imdb_df["numVotes"] = imdb_df["numVotes"].fillna(0)

# --- BUILD TITLE INDEX ---
imdb_primary = imdb_df[["primaryTitle", "startYear", "tconst", "numVotes"]].copy()
imdb_primary["match_title"] = imdb_primary["primaryTitle"].str.lower().str.strip()

imdb_original = imdb_df[["originalTitle", "startYear", "tconst", "numVotes"]].copy()
imdb_original["match_title"] = imdb_original["originalTitle"].str.lower().str.strip()

imdb_combined = pd.concat([imdb_primary, imdb_original])[["match_title", "startYear", "tconst", "numVotes"]]
imdb_combined = imdb_combined.drop_duplicates(subset=["match_title", "tconst"])

# Group into a dict: title -> list of (startYear, tconst, numVotes)
title_index = {
    title: list(zip(group["startYear"], group["tconst"], group["numVotes"]))
    for title, group in imdb_combined.groupby("match_title")
}

# --- MATCHING LOGIC ---
bo_df["bo_year"] = pd.to_datetime(bo_df["date"]).dt.year

def find_tconst(match_title, bo_year):
    candidates = title_index.get(match_title, [])
    # Only keep movies released within the 3 years before the BO date
    valid = [
        (start_year, tconst, num_votes)
        for start_year, tconst, num_votes in candidates
        if (bo_year - 3) <= start_year <= bo_year
    ]
    if not valid:
        return None
    # Pick the one with the most votes
    return max(valid, key=lambda x: x[2])[1]

bo_df["imdb_tconst"] = bo_df.apply(
    lambda row: find_tconst(row["match_title"], row["bo_year"]), axis=1
)

# --- FINAL CLEANUP ---
bo_df = bo_df.sort_values(by=["date", "td"], ascending=[True, True])
bo_df = bo_df.drop(columns=["match_title", "release", "bo_year"])
bo_df = bo_df[bo_df["imdb_tconst"].notna()]

# --- SAVE ---
bo_df.to_csv(output_base_path + output_file, index=False)

print(f"Done! Matched {bo_df['imdb_tconst'].notna().sum()} rows out of {len(bo_df)}")