import numpy as np
import pandas as pd
import re

# --- FILE PATHS ---
netflix_file = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\all-weeks-countries.csv"
imdb_file = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\used\\filtered_title_basics.tsv"
ratings_file = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\used\\filtered_title_ratings.tsv"
output_file = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\used\\countries-with-imdb.csv"

# --- LOAD DATA ---
netflix_df = pd.read_csv(netflix_file, encoding="latin-1")
imdb_df = pd.read_csv(imdb_file, sep="\t", low_memory=False)
ratings_df = pd.read_csv(ratings_file, sep="\t", low_memory=False)

# --- CONVERT SEASON TITLES ---
for i, row in netflix_df.iterrows():
    if str(row['season_title']) != 'nan':
        numbers_in_title = re.findall("[0-9]+", row['season_title'])
        if not numbers_in_title or int(numbers_in_title[-1]) > 40:
            season = 1
        else:
            season = int(numbers_in_title[-1])
        netflix_df.at[i, 'season_title'] = season

# --- PARSE WEEK ---
netflix_df["bo_year"] = pd.to_datetime(netflix_df["week"]).dt.year

# --- CLEAN TITLES ---
netflix_df["match_title"] = netflix_df["show_title"].str.lower().str.strip()
imdb_df["startYear"] = pd.to_numeric(imdb_df["startYear"], errors="coerce")

# --- MERGE RATINGS ---
imdb_df = imdb_df.merge(ratings_df[["tconst", "numVotes"]], on="tconst", how="left")
imdb_df["numVotes"] = imdb_df["numVotes"].fillna(0)

# --- CATEGORY -> IMDB TYPES ---
def get_expected_types(category):
    if isinstance(category, str) and "film" in category.lower():
        return ["movie"]
    elif isinstance(category, str) and "tv" in category.lower():
        return ["tvSeries", "tvMiniSeries"]
    return []

# --- BUILD TITLE INDEX PER CATEGORY TYPE ---
# We build two separate indexes: one for movies, one for TV
def build_index(imdb_df, types):
    subset = imdb_df[imdb_df["titleType"].isin(types)].copy()

    primary = subset[["primaryTitle", "startYear", "tconst", "numVotes"]].copy()
    primary["match_title"] = primary["primaryTitle"].str.lower().str.strip()

    original = subset[["originalTitle", "startYear", "tconst", "numVotes"]].copy()
    original["match_title"] = original["originalTitle"].str.lower().str.strip()

    combined = pd.concat([primary, original])[["match_title", "startYear", "tconst", "numVotes"]]
    combined = combined.drop_duplicates(subset=["match_title", "tconst"])

    return {
        title: list(zip(group["startYear"], group["tconst"], group["numVotes"]))
        for title, group in combined.groupby("match_title")
    }

movie_index = build_index(imdb_df, ["movie"])
tv_index = build_index(imdb_df, ["tvSeries", "tvMiniSeries"])

# --- MATCHING LOGIC ---
def find_tconst(match_title, bo_year, category):
    if isinstance(category, str) and "film" in category.lower():
        index = movie_index
    elif isinstance(category, str) and "tv" in category.lower():
        index = tv_index
    else:
        return None

    candidates = index.get(match_title, [])
    valid = [
        (start_year, tconst, num_votes)
        for start_year, tconst, num_votes in candidates
        if (bo_year - 3) <= start_year <= bo_year
    ]
    if not valid:
        return None
    return max(valid, key=lambda x: x[2])[1]

netflix_df["imdb_tconst"] = netflix_df.apply(
    lambda row: find_tconst(row["match_title"], row["bo_year"], row["category"]), axis=1
)

# --- FINAL CLEANUP ---
netflix_df = netflix_df[netflix_df["imdb_tconst"].notna()]
netflix_df = netflix_df.drop(columns=["match_title", "show_title", "bo_year"])

# --- SAVE ---
netflix_df.to_csv(output_file, index=False)