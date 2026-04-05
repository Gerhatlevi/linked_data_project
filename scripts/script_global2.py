import pandas as pd

# --- FILE PATHS ---
global_file = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\all-weeks-global.csv"
imdb_file = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\used\\filtered_title_basics.tsv"
ratings_file = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\used\\filtered_title_ratings.tsv"
output_file = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\used\\global-with-imdb2.csv"

# --- LOAD DATA ---
df = pd.read_csv(global_file, encoding="latin-1")
imdb_df = pd.read_csv(imdb_file, sep="\t", low_memory=False)
ratings_df = pd.read_csv(ratings_file, sep="\t", low_memory=False)

# --- PARSE ---
df["bo_year"] = pd.to_datetime(df["week"]).dt.year
imdb_df["startYear"] = pd.to_numeric(imdb_df["startYear"], errors="coerce")
imdb_df = imdb_df.merge(ratings_df[["tconst", "numVotes"]], on="tconst", how="left")
imdb_df["numVotes"] = imdb_df["numVotes"].fillna(0)

# --- CLEAN TITLES ---
df["match_title"] = df["show_title"].str.lower().str.strip()

# --- BUILD INDEXES ---
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

# --- MATCHING ---
def find_tconst(match_title, bo_year, category):
    if "film" in category.lower():
        index = movie_index
    elif "tv" in category.lower():
        index = tv_index
    else:
        return None
    candidates = index.get(match_title, [])
    valid = [
        (start_year, tconst, num_votes)
        for start_year, tconst, num_votes in candidates
        if start_year <= bo_year
    ]
    if not valid:
        return None
    return max(valid, key=lambda x: x[2])[1]

df["imdb_tconst"] = df.apply(
    lambda row: find_tconst(row["match_title"], row["bo_year"], row["category"]), axis=1
)

# --- CLEANUP ---
df = df[df["imdb_tconst"].notna()]
cols_to_drop = [c for c in ["match_title", "bo_year"] if c in df.columns]
df = df.drop(columns=cols_to_drop)

df.to_csv(output_file, index=False)
print(f"Done! Saved {len(df)} rows to {output_file}")