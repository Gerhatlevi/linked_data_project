import pandas as pd
import os

def filter_ratings():
    # Elérési út beállítása
    base_path = r'C:\Users\leven\Erasmus\3_quartile\LDSW\Project'

    basics_file = os.path.join(base_path, 'filtered_title_basics.tsv')
    ratings_file = os.path.join(base_path, 'title.ratings.tsv')
    output_file = os.path.join(base_path, 'filtered_title_ratings.tsv')

    print("Szűrt filmek azonosítóinak betöltése...")
    try:
        # Csak a tconst oszlopot olvassuk be, hogy spóroljunk a memóriával
        basics_df = pd.read_csv(basics_file, sep='\t', usecols=['tconst'])
        valid_tconsts = set(basics_df['tconst'])
        print(f"{len(valid_tconsts)} érvényes azonosító betöltve.")
    except FileNotFoundError:
        print(f"Hiba: A {basics_file} nem található!")
        return
    except Exception as e:
        print(f"Hiba történt a basics fájl beolvasásakor: {e}")
        return

    print("Ratings fájl szűrése...")
    
    # A ratings fájl általában kisebb, de a biztonság kedvéért itt is chunk-okat használunk
    chunk_size = 200000
    first_chunk = True

    try:
        reader = pd.read_csv(ratings_file, sep='\t', chunksize=chunk_size, low_memory=False)

        for i, chunk in enumerate(reader):
            # Csak azokat a sorokat tartjuk meg, amiknek a tconst-ja benne van a halmazunkban
            filtered_chunk = chunk[chunk['tconst'].isin(valid_tconsts)]

            if first_chunk:
                filtered_chunk.to_csv(output_file, sep='\t', index=False, mode='w', encoding='utf-8')
                first_chunk = False
            else:
                filtered_chunk.to_csv(output_file, sep='\t', index=False, mode='a', header=False, encoding='utf-8')

            if (i + 1) % 10 == 0:
                print(f"Feldolgozva: {(i + 1) * chunk_size} sor a ratings fájlból...")

        print(f"\nKész! A szűrt értékelések mentve: {output_file}")

    except Exception as e:
        print(f"Hiba történt a ratings feldolgozása során: {e}")

if __name__ == "__main__":
    filter_ratings()