import pandas as pd

path = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\"
outpath = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\final_files\\"

def clean_box_office(input_path, output_path):
    df = pd.read_csv(input_path, low_memory=False)
    df = df.drop(columns=['estimated', 'newthisday', 'yd', 'lw'], errors='ignore')
    df.to_csv(output_path, index=False, encoding='utf-8')

def clean_names(input_path, output_path):
    df = pd.read_csv(input_path, low_memory=False)
    df = df.drop(columns=['primaryProfession'], errors='ignore')
    df.to_csv(output_path, index=False, encoding='utf-8')


if __name__ == "__main__":
    clean_box_office(path + 'boxoffice-with-imdb.csv', outpath + 'box_office_with-imdb.csv')
    clean_names(path + 'filtered_name_basics.tsv', outpath + 'names_basics.tsv')