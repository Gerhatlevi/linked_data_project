import pandas as pd
import os

def filter_names():
    # Elérési út beállítása
    base_path = r'C:\Users\leven\Erasmus\3_quartile\LDSW\Project'
    base_github = r'C:\Users\leven\Erasmus\3_quartile\LDSW\Project\linked_data_project'

    basics_file = os.path.join(base_github, 'filtered_title_basics.tsv')
    names_file = os.path.join(base_path, 'name.basics.tsv')
    output_file = os.path.join(base_path, 'filtered_name_basics.tsv')

    print("Érvényes film azonosítók betöltése...")
    try:
        # Beolvassuk a már megszűrt tconst-okat
        basics_df = pd.read_csv(basics_file, sep='\t', usecols=['tconst'])
        valid_tconsts = set(basics_df['tconst'])
        print(f"{len(valid_tconsts)} érvényes film azonosító betöltve.")
    except FileNotFoundError:
        print(f"Hiba: A {basics_file} nem található!")
        return

    print("Személyek szűrése (ez eltarthat egy ideig a lista-szétbontás miatt)...")
    
    chunk_size = 100000
    first_chunk = True

    try:
        # A quoting=3 itt is fontos lehet a nevekben lévő esetleges speciális karakterek miatt
        reader = pd.read_csv(names_file, sep='\t', chunksize=chunk_size, low_memory=False, quoting=3)

        for i, chunk in enumerate(reader):
            # 1. Kidobjuk azokat, ahol nincs megadva egyetlen cím sem (\N)
            # 2. Megnézzük, hogy a knownForTitles-ben lévő ID-k közül bármelyik benne van-e a szűrt listánkban
            
            def has_valid_title(known_titles):
                if pd.isna(known_titles) or known_titles == r'\N':
                    return False
                # Szétbontjuk a vessző mentén és ellenőrizzük az egyezést
                titles_list = known_titles.split(',')
                return any(t in valid_tconsts for t in titles_list)

            # Szűrés alkalmazása a chunk-ra
            filtered_chunk = chunk[chunk['knownForTitles'].apply(has_valid_title)]

            birth_year = pd.to_numeric(filtered_chunk['birthYear'], errors='coerce')
            filtered_chunk = filtered_chunk[(birth_year >= 1950)]

            if first_chunk:
                filtered_chunk.to_csv(output_file, sep='\t', index=False, mode='w', encoding='utf-8')
                first_chunk = False
            else:
                filtered_chunk.to_csv(output_file, sep='\t', index=False, mode='a', header=False, encoding='utf-8')

            if (i + 1) % 10 == 0:
                print(f"Feldolgozva: {(i + 1) * chunk_size} név a listából...")

        print(f"\nKész! A szűrt névsor mentve: {output_file}")

    except Exception as e:
        print(f"Hiba történt a feldolgozás során: {e}")

if __name__ == "__main__":
    filter_names()