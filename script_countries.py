import numpy as np
import pandas as pd
import re

# --- FILE PATHS ---
netflix_file = "all-weeks-countries.csv" # Az új országos fájlod
imdb_file = "filtered_title_basics.tsv"
output_file = "countries-with-imdb.csv"


# --- LOAD DATA ---
netflix_df = pd.read_csv(netflix_file, encoding="latin-1")
imdb_df = pd.read_csv(imdb_file, sep="\t", low_memory=False)

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

# --- CLEAN TITLES & CATEGORIES ---
netflix_df["match_title"] = netflix_df["show_title"].str.lower().str.strip()
imdb_df["match_title"] = imdb_df["primaryTitle"].str.lower().str.strip()
imdb_df["match_title2"] = imdb_df["originalTitle"].str.lower().str.strip()

# --- UNIQUE SHOWS MATCHING (Efficiency Boost) ---
# Kigyűjtjük az egyedi cím-kategória párosokat, hogy ne keressünk 100-szor ugyanarra
unique_shows = netflix_df[["match_title", "category"]].drop_duplicates()

def get_expected_types(category):
    if isinstance(category, str) and "film" in category.lower():
        return ["movie"]
    elif isinstance(category, str) and "tv" in category.lower():
        return ["tvSeries", "tvMiniSeries"]
    return []

# Keressük meg a tconst-ot az egyedi listára
results = []
for _, row in unique_shows.iterrows():
    expected = get_expected_types(row["category"])
    
    candidates = imdb_df[
        ((imdb_df["match_title"] == row["match_title"]) | 
         (imdb_df["match_title2"] == row["match_title"])) &
        (imdb_df["titleType"].isin(expected))
    ]
    
    if not candidates.empty:
        # Ha több van, a legújabbra tippeljünk (startYear szerint csökkenő)
        best_tconst = candidates.sort_values("startYear", ascending=False).iloc[0]["tconst"]
        results.append({"match_title": row["match_title"], "category": row["category"], "imdb_tconst": best_tconst})
    else:
        results.append({"match_title": row["match_title"], "category": row["category"], "imdb_tconst": None})

match_lookup = pd.DataFrame(results)

# --- MERGE BACK TO MAIN DF ---
# Visszarakjuk az eredeti nagy táblázathoz
netflix_df = pd.merge(netflix_df, match_lookup, on=["match_title", "category"], how="left")

# --- FINAL CLEANUP & SORT ---
# Maradjon meg minden, de ahol nincs tconst, ott NaN (vagy üres)
netflix_df = netflix_df[netflix_df["imdb_tconst"].notna()]

# Sorrend visszaállítása (Ország, Hét, Rank szerint)
netflix_df = netflix_df.sort_values(by=["country_name", "week", "weekly_rank"])

# Segédoszlop törlése
netflix_df = netflix_df.drop(columns=["match_title"])

# --- SAVE ---
netflix_df.to_csv(output_file, index=False)

print(f"Done! Saved {len(netflix_df)} rows to {output_file}")