import pandas as pd
import numpy as np

# TODO: imdb_tconst refers to the entire show, not just the season, so incorporate season data

# Get the viewership data for a movie/show, returned as an array with:
# [Weekly hours viewed, weekly views, cumulative weeks in top 10
def extract_viewership(at_rank):
    cols = ['weekly_hours_viewed', 'weekly_views', 'cumulative_weeks_in_top_10']
    result = []
    for col in cols:
        result.append(float(at_rank[col].values[0]))
    return result


df = pd.read_csv("../data_files/countries-with-imdb.csv", encoding="latin-1")

# Create the row indices, one for each week's best movie/TV, per country
weeks = df["week"].unique()
countries = df["country_name"].unique()
row_indices = []
for week in weeks:
    for country in countries:
        row_indices.append(f'{week}_{country}_Films')
        row_indices.append(f'{week}_{country}_TV')


columns = ['week', 'country']
# Create the columns, 20 for each week, the movie/show at each rank + cumulative weeks in top 10
for rank in range(1, 11):
    columns.append('at_rank' + str(rank))
    columns.append('cumulative_weeks_in_top_10_' + str(rank))

output = pd.DataFrame(index = row_indices, columns = columns)

# Iterate over every week + movie/TV + English/non-English
for index, row in output.iterrows():
    attrs = index.split('_')
    week = attrs[0]
    country = attrs[1]
    category = attrs[2]
    at_week = df.loc[df['week'] == week]
    in_country = at_week.loc[at_week['country_name'] == country]
    in_category = in_country.loc[in_country['category'] == category]
    # Iterate over the entry at every rank
    for rank in range(1, 11):
        at_rank = in_category.loc[in_category['weekly_rank'] == rank]
        if not at_rank.empty:
            # Store the movie at rank, plus the weeks in top 10
            row['at_rank' + str(rank)] = at_rank['imdb_tconst'].values[0]
            row['cumulative_weeks_in_top_10_' + str(rank)] = at_rank['cumulative_weeks_in_top_10'].values[0]
            # print(rank)
            # print(at_rank['imdb_tconst'].values[0])
            # print(at_rank['cumulative_weeks_in_top_10'].values[0])
    row['week'] = week
    row['country'] = country
    output.loc[index] = row

output.to_csv("../data_filescountries_reformatted.csv", index=False)

