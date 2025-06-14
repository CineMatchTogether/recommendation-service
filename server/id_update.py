import csv
import psycopg2
import re

def normalize(name):
    name = name.lower()
    name = re.sub(r'[^a-z0-9]+', ' ', name)  # удаляем спецсимволы, оставляем буквы и цифры
    name = re.sub(r'\s+', ' ', name).strip()  # удаляем лишние пробелы
    return name

def extract_en_name_and_year(title):
    match = re.search(r'(.+?)\s*\((\d{4})\)', title)
    if match:
        name = match.group(1).strip()
        year = int(match.group(2))
        return normalize(name), year
    return normalize(title), None

# Подключение к базе данных
conn = psycopg2.connect(
    dbname="movie-service-db",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5433"
)
cursor = conn.cursor()

# Получаем маппинг (нормализованное название, год) -> id
cursor.execute("""
    SELECT id, en_name, year 
    FROM movies 
    WHERE en_name IS NOT NULL AND year IS NOT NULL
    ORDER BY imdb_rating DESC, kp_rating DESC
    LIMIT 13000
""")
movies_db = cursor.fetchall()
normalized_mapping = {
    (normalize(en_name), year): id
    for id, en_name, year in movies_db
}

input_file = "movies_large.csv"
output_file = "movies_large_updated.csv"

match_count = 0
total_count = 0

with open(input_file, mode='r', encoding='utf-8') as infile, \
     open(output_file, mode='w', encoding='utf-8', newline='') as outfile:
    
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames + ['db_id']
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        total_count += 1
        norm_en_name, year = extract_en_name_and_year(row['title'])
        movie_id = normalized_mapping.get((norm_en_name, year))
        row['db_id'] = movie_id if movie_id is not None else ''
        
        if movie_id is not None:
            match_count += 1
            writer.writerow(row)
        else:
            print(f"Не найдено: {row['title']} -> ({norm_en_name}, {year})")

print(f"Всего записей: {total_count}")
print(f"Совпадений: {match_count}")
print(f"Без совпадений (удалено): {total_count - match_count}")

cursor.close()
conn.close()