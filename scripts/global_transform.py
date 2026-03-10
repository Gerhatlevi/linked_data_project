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


df = pd.read_csv("../data_files/global-with-imdb.csv", encoding="latin-1")

# Create the row indices, one for each week's best movie/TV, English/Non-English
weeks = df["week"].unique()
row_indices = []
for week in weeks:
    row_indices.append(week + '_Films_English')
    row_indices.append(week + '_Films_Non-English')
    row_indices.append(week + '_TV_English')
    row_indices.append(week + '_TV_Non-English')

columns = ['week', 'category', 'language']
# Create the columns, 20 for each week, the movie/show at each rank + viewership at each rank
for rank in range(1, 11):
    columns.append('at_rank' + str(rank))
    columns.append('views_at_rank' + str(rank))

output = pd.DataFrame(index = row_indices, columns = columns)

# Iterate over every week + movie/TV + English/non-English
for index, row in output.iterrows():
    attrs = index.split('_')
    week = attrs[0]
    category = attrs[1] + ' (' + attrs[2] + ')'
    at_week = df.loc[df['week'] == week]
    in_category = at_week.loc[at_week['category'] == category]
    # Iterate over the entry at every rank
    for rank in range(1, 11):
        at_rank = in_category.loc[in_category['weekly_rank'] == rank]
        if not at_rank.empty:
             # Store the movie at rank, plus the viewership data
            row['at_rank' + str(rank)] = at_rank['imdb_tconst'].values[0]
            row['views_at_rank' + str(rank)] = extract_viewership(at_rank)
    row['week'] = week
    row['category'] = attrs[1]
    row['language'] = attrs[2]

output.to_csv("../data_files/global_reformatted.csv")

