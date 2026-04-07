import pandas as pd
import os

def filter_imdb_data():
    base_path = r'C:\Users\leven\Erasmus\3_quartile\LDSW\Project'

    input_tsv = 'title.basics.tsv'
    titles_file = 'unique_titles.txt'
    output_tsv = 'filtered_title_basics.tsv'

    print("Listád betöltése...")
    try:
        with open(os.path.join(base_path, titles_file), 'r', encoding='utf-8') as f:
            my_titles = set(line.strip() for line in f)
    except FileNotFoundError:
        print(f"Hiba: A {titles_file} nem található!")
        return

    print(f"{len(my_titles)} egyedi cím betöltve. Szűrés indítása...")

    chunk_size = 100000 
    first_chunk = True

    try:
        reader = pd.read_csv(os.path.join(base_path, input_tsv), sep='\t', chunksize=chunk_size, low_memory=False, quoting=3)

        for i, chunk in enumerate(reader):
            filtered_chunk = chunk[
                chunk['primaryTitle'].str.lower().isin(my_titles) | 
                chunk['originalTitle'].str.lower().isin(my_titles)
            ]

            if first_chunk:
                filtered_chunk.to_csv(os.path.join(base_path, output_tsv), sep='\t', index=False, mode='w', encoding='utf-8')
                first_chunk = False
            else:
                filtered_chunk.to_csv(os.path.join(base_path, output_tsv), sep='\t', index=False, mode='a', header=False, encoding='utf-8')

            if (i + 1) % 10 == 0:
                print(f"Feldolgozva: {(i + 1) * chunk_size} sor...")

        print(f"\nKész! A szűrt adatok mentve: {output_tsv}")

    except Exception as e:
        print(f"Hiba történt a feldolgozás során: {e}")

if __name__ == "__main__":
    filter_imdb_data()