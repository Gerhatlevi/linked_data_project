import pandas as pd

def collect_titles():
    files_and_columns = {
        'box_office.csv': 'release',
        'most-popular.csv': 'show_title',
        'all-weeks-global.csv': 'show_title',
        'all-weeks-countries.csv': 'show_title'
    }

    all_titles = set()

    for file_name, column_name in files_and_columns.items():
        try:
            try:
                df = pd.read_csv(f"C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\{file_name}")
            except UnicodeDecodeError:
                df = pd.read_csv(f"C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\{file_name}", encoding='latin-1')
            
            if column_name in df.columns:
                titles = df[column_name].dropna().unique()
                all_titles.update(titles)
                print(f"Sikeresen beolvasva: {file_name} ({len(titles)} cím)")
            else:
                print(f"Hiba: A '{column_name}' oszlop nem található a(z) {file_name} fájlban.")
        
        except FileNotFoundError:
            print(f"Figyelem: A(z) {file_name} fájl nem található, kihagyás...")
        except Exception as e:
            print(f"Hiba történt a(z) {file_name} feldolgozásakor: {e}")

    sorted_titles = sorted(list(all_titles))
    
    print(f"\nÖsszesen {len(sorted_titles)} egyedi címet találtam.")
    
    with open('unique_titles.txt', 'w', encoding='utf-8') as f:
        for title in sorted_titles:
            title = title.strip().lower()
            f.write(f"{title}\n")
    
    print("A címek elmentve a 'unique_titles.txt' fájlba.")

if __name__ == "__main__":
    collect_titles()