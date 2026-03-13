import pandas as pd

path = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\"

def clean_box_office(input_path, output_path):
    df = pd.read_csv(input_path, low_memory=False)
    df = df.drop(columns=['estimated', 'newthisday', 'yd', 'lw', 'daily', 'avg', 'days'], errors='ignore')
    df.to_csv(output_path, index=False, encoding='utf-8')

def clean_names(input_path, output_path):
    df = pd.read_csv(input_path, low_memory=False, sep='\t')
    df = df.drop(columns=['primaryProfession'], errors='ignore')
    df.to_csv(output_path, sep='\t', index=False, encoding='utf-8')

def clean_title_basics(input_path, output_path):
    df = pd.read_csv(input_path, sep='\t')
    df["genres"] = df["genres"].str.split(",").str[0]    
    df.to_csv(output_path, sep='\t', index=False, encoding='utf-8')

def clean_global(input_path, output_path):
    df = pd.read_csv(input_path, low_memory=False)
    df = df.drop(columns=['is_staggered_launch', 'weekly_hours_viewed', 'cumulative_weeks_in_top_10'], errors='ignore')
    df = df.drop(columns=['is_staggered_launch'], errors='ignore')
    df.to_csv(output_path, index=False, encoding='utf-8')

def clean_most_popular(input_path, output_path):
    df = pd.read_csv(input_path, low_memory=False)
    df = df.drop(columns=['hours_viewed_first_91_days'], errors='ignore')
    df.to_csv(output_path, index=False, encoding='utf-8')

def clean_countries(input_path, output_path):
    df = pd.read_csv(input_path, low_memory=False)
    df = df.drop(columns=['cumulative_weeks_in_top_10'], errors='ignore')
    df.to_csv(output_path, index=False, encoding='utf-8')


if __name__ == "__main__":
    clean_box_office(path + "box_office_with-imdb.csv", path + "box_office_with-imdb.csv")