import csv
import psycopg2

# Подключение к базе данных Postgres
conn = psycopg2.connect(
    dbname="movie-service-db",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5433"
)
cursor = conn.cursor()

# Получаем маппинг en_name -> id из БД
cursor.execute("SELECT id, en_name FROM movies WHERE en_name IS NOT NULL")
movies_db = cursor.fetchall()
en_name_to_id = {en_name.lower(): id for id, en_name in movies_db}

# Функция для извлечения en_name из title CSV
def extract_en_name(title):
    if '(' in title:
        return title[:title.rfind('(')].strip().lower()
    return title.strip().lower()

# Чтение и добавление столбца db_id
input_file = "movies (1).csv"
output_file = "movies 2.csv"

match_count = 0  # Счетчик совпадений
total_count = 0  # Общее количество записей

with open(input_file, mode='r', encoding='utf-8') as infile, \
     open(output_file, mode='w', encoding='utf-8', newline='') as outfile:
    
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames + ['db_id']  # Добавляем новый столбец
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        total_count += 1
        en_name = extract_en_name(row['title'])
        movie_id = en_name_to_id.get(en_name)
        row['db_id'] = movie_id if movie_id is not None else ''
        
        if movie_id is not None:
            match_count += 1
            writer.writerow(row)  # Записываем только строки с совпадением
        else:
            print(f"Не найдено: {row['title']} -> {en_name}")

# Вывод статистики
print(f"Всего записей: {total_count}")
print(f"Совпадений: {match_count}")
print(f"Без совпадений (удалено): {total_count - match_count}")

cursor.close()
conn.close()