import csv
import random

# Чтение db_id из movies.csv
movies_file = "movies.csv"
valid_movie_ids = set()

with open(movies_file, mode='r', encoding='utf-8') as movies_infile:
    reader = csv.DictReader(movies_infile)
    for row in reader:
        if row['db_id']:  # Проверяем, что db_id не пустой
            valid_movie_ids.add(int(row['db_id']))

# Чтение всех транзакций и группировка по пользователям
input_rating_file = "ratings 2.csv"
output_rating_file = "ratings_2.csv"

# Собираем транзакции по userId, фильтруя только совпадения
user_transactions = {}
total_count = 0  # Общее количество транзакций
match_count = 0  # Счетчик совпадающих транзакций

with open(input_rating_file, mode='r', encoding='utf-8') as infile:
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames  # Сохраняем заголовки (userId, movieId, rating, timestamp)
    
    for row in reader:
        total_count += 1
        movie_id = int(row['movieId'])
        if movie_id in valid_movie_ids:  # Сохраняем только совпадающие транзакции
            user_id = row['userId']
            if user_id not in user_transactions:
                user_transactions[user_id] = []
            user_transactions[user_id].append(row)
            match_count += 1

# Фильтрация пользователей с вероятностью 50%
keep_probability = 1
filtered_transactions = []
kept_users = 0
total_users = len(user_transactions)

for user_id, transactions in user_transactions.items():
    if random.random() < keep_probability:
        filtered_transactions.extend(transactions)
        kept_users += 1

# Запись отфильтрованных транзакций в ratings.csv
with open(output_rating_file, mode='w', encoding='utf-8', newline='') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in filtered_transactions:
        writer.writerow(row)

# Вывод статистики
print(f"Всего транзакций: {total_count}")
print(f"Совпадающих транзакций: {match_count}")
print(f"Оставлено транзакций: {len(filtered_transactions)}")
print(f"Удалено транзакций: {match_count - len(filtered_transactions)}")
print(f"Всего пользователей: {total_users}")
print(f"Оставлено пользователей: {kept_users}")
print(f"Удалено пользователей: {total_users - kept_users}")