import pandas as pd
import os

def filter_episodes():
    # Elérési út beállítása
    base_path = r'C:\Users\leven\Erasmus\3_quartile\LDSW\Project'

    basics_file = os.path.join(base_path, 'filtered_title_basics.tsv')
    episodes_file = os.path.join(base_path, 'title.episode.tsv')
    output_file = os.path.join(base_path, 'filtered_title_episodes.tsv')

    print("Érvényes azonosítók betöltése...")
    try:
        # Beolvassuk a már megszűrt tconst-okat
        basics_df = pd.read_csv(basics_file, sep='\t', usecols=['tconst'])
        valid_tconsts = set(basics_df['tconst'])
        print(f"{len(valid_tconsts)} érvényes azonosító készen áll.")
    except FileNotFoundError:
        print(f"Hiba: A {basics_file} nem található!")
        return

    print("Epizódok szűrése...")
    
    chunk_size = 200000
    first_chunk = True

    try:
        # Az IMDb fájlokban néha hiányozhatnak adatok (\N), a low_memory segít ezen
        reader = pd.read_csv(episodes_file, sep='\t', chunksize=chunk_size, low_memory=False)

        for i, chunk in enumerate(reader):
            # Szűrés: Megtartjuk, ha az epizód (tconst) VAGY a sorozata (parentTconst) benne van a listában
            filtered_chunk = chunk[
                chunk['tconst'].isin(valid_tconsts) | 
                chunk['parentTconst'].isin(valid_tconsts)
            ]

            if first_chunk:
                filtered_chunk.to_csv(output_file, sep='\t', index=False, mode='w', encoding='utf-8')
                first_chunk = False
            else:
                filtered_chunk.to_csv(output_file, sep='\t', index=False, mode='a', header=False, encoding='utf-8')

            if (i + 1) % 10 == 0:
                print(f"Feldolgozva: {(i + 1) * chunk_size} sor az epizódokból...")

        print(f"\nSiker! A szűrt epizódlista mentve: {output_file}")

    except Exception as e:
        print(f"Hiba történt a feldolgozás során: {e}")

if __name__ == "__main__":
    filter_episodes()