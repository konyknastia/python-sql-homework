import sqlite3
import datetime

# Назва файлу бази даних
DB_NAME = "salon_database.db"


def create_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def execute_script(conn, script):
    cursor = conn.cursor()
    cursor.executescript(script)
    conn.commit()


def run_query(conn, query, title="Результат запиту"):
    cursor = conn.cursor()
    cursor.execute(query)

    columns = [description[0] for description in cursor.description]
    rows = cursor.fetchall()

    print(f"\n--- {title} ---")
    print(f"{' | '.join(columns)}")
    print("-" * 60)

    if not rows:
        print("(Немає даних)")
    else:
        for row in rows:
            print(" | ".join(str(item) for item in row))
    print("-" * 60)


def main():
    conn = create_connection()
    print("1. База даних підключена.")

    drop_and_create_sql = """
    -- Очищення
    DROP TABLE IF EXISTS appointments;
    DROP TABLE IF EXISTS masters;
    DROP TABLE IF EXISTS clients;
    DROP TABLE IF EXISTS services;

    -- Таблиця майстрів
    CREATE TABLE masters (
        id            INTEGER PRIMARY KEY,
        name          TEXT,
        specialization TEXT,
        work_hours    TEXT 
    );

    -- Таблиця клієнтів
    CREATE TABLE clients (
        id            INTEGER PRIMARY KEY,
        full_name     TEXT,
        email         TEXT UNIQUE,
        phone_number  TEXT
    );

    -- Таблиця послуг
    CREATE TABLE services (
        id            INTEGER PRIMARY KEY,
        name          TEXT,
        price         REAL,
        duration      INTEGER, -- тривалість у хвилинах
        description   TEXT
    );

    -- Таблиця записів (зв'язуюча)
    CREATE TABLE appointments (
        id            INTEGER PRIMARY KEY,
        date          TEXT,      -- 'datetime' у SQLite зберігається як TEXT (YYYY-MM-DD HH:MM)
        status        TEXT CHECK (status IN ('booked', 'completed', 'canceled')) NOT NULL,
        master_id     INTEGER,
        client_id     INTEGER,
        service_id    INTEGER,
        FOREIGN KEY (master_id) REFERENCES masters(id),
        FOREIGN KEY (client_id) REFERENCES clients(id),
        FOREIGN KEY (service_id) REFERENCES services(id)
    );
    """

    print("   Створення таблиць для схеми салону краси...")
    execute_script(conn, drop_and_create_sql)


    print("2. Наповнення бази даними...")
    insert_data_sql = """
    -- Майстри
    INSERT INTO masters (name, specialization, work_hours) VALUES
    ('Олена Коваль', 'Стиліст', '10:00-19:00'), -- id=1
    ('Ірина Мельник', 'Манікюр', '09:00-18:00'), -- id=2
    ('Андрій Сидоренко', 'Барбер', '11:00-20:00'), -- id=3
    ('Марія Лисенко', 'Стиліст', '10:00-16:00'); -- id=4

    -- Клієнти
    INSERT INTO clients (full_name, email, phone_number) VALUES
    ('Анна Петренко', 'anna.p@test.com', '0981234567'), -- id=1
    ('Богдан Іванов', 'bohdan.i@test.com', '0509876543'), -- id=2
    ('Вікторія Семенко', 'viktoria.s@test.com', '0671122334'), -- id=3
    ('Дмитро Кравчук', 'dmitro.k@test.com', '0937778899'); -- id=4

    -- Послуги
    INSERT INTO services (id, name, price, duration, description) VALUES
    (101, 'Жіноча стрижка', 450.00, 60, 'Стрижка, миття, укладка'),
    (102, 'Манікюр класичний', 300.00, 90, 'З покриттям гель-лаком'),
    (103, 'Чоловіча стрижка', 350.00, 45, 'Стрижка, миття'),
    (104, 'Фарбування волосся', 1500.00, 120, 'Складне фарбування');

    -- Записи (appointments)
    INSERT INTO appointments (date, status, master_id, client_id, service_id) VALUES
    ('2025-12-05 15:00', 'booked', 1, 1, 101), 
    ('2025-12-05 10:30', 'completed', 2, 2, 102),
    ('2025-12-06 18:00', 'booked', 3, 3, 103),
    ('2025-12-06 12:00', 'canceled', 1, 3, 101), 
    ('2025-11-20 14:00', 'completed', 1, 4, 104), -- Стара завершена послуга
    ('2025-12-07 11:00', 'booked', 2, 4, 102),
    ('2025-12-07 16:00', 'booked', 3, 1, 103);
    """
    execute_script(conn, insert_data_sql)


    # 1. Показати всі записи з ім'ям клієнта, майстра та назвою послуги.
    query_1 = """
    SELECT 
        a.date, c.full_name AS Client, m.name AS Master, s.name AS Service, a.status
    FROM appointments a
    JOIN clients c ON a.client_id = c.id
    JOIN masters m ON a.master_id = m.id
    JOIN services s ON a.service_id = s.id
    ORDER BY a.date;
    """
    run_query(conn, query_1, "1. Усі записи з деталями (JOIN)")

    # 2. Знайти всі записи, заплановані на '2025-12-06'.
    query_2 = """
    SELECT * FROM appointments
    WHERE date LIKE '2025-12-06%';
    """
    run_query(conn, query_2, "2. Записи на 2025-12-06")

    # 3. Список послуг з ціною вищою за 400.00 грн.
    query_3 = """
    SELECT name, price, duration FROM services
    WHERE price > 400.00;
    """
    run_query(conn, query_3, "3. Послуги дорожчі за 400 грн")

    # 4. Знайти клієнтів з поштою на домені 'test.com'.
    query_4 = """
    SELECT full_name, email FROM clients
    WHERE email LIKE '%@test.com';
    """
    run_query(conn, query_4, "4. Клієнти з домену test.com")

    # 5. Записи зі статусом 'booked' або 'canceled'.
    query_5 = """
    SELECT date, status FROM appointments
    WHERE status IN ('booked', 'canceled');
    """
    run_query(conn, query_5, "5. Записи зі статусом 'booked' або 'canceled'")

    # 6. Порахувати кількість клієнтів у кожному статусі запису.
    query_6 = """
    SELECT status, COUNT(*) AS appointment_count
    FROM appointments
    GROUP BY status
    ORDER BY appointment_count DESC;
    """
    run_query(conn, query_6, "6. Кількість записів по статусах")

    # 7. Середня ціна послуг по спеціалізаціях майстрів (агрегація через JOIN).
    query_7 = """
    SELECT m.specialization, ROUND(AVG(s.price), 2) AS avg_service_price
    FROM appointments a
    JOIN masters m ON a.master_id = m.id
    JOIN services s ON a.service_id = s.id
    GROUP BY m.specialization
    ORDER BY avg_service_price DESC;
    """
    run_query(conn, query_7, "7. Середня ціна послуг по спеціалізаціях")

    # 8. Топ-2 найдорожчі послуги.
    query_8 = """
    SELECT name, price
    FROM services
    ORDER BY price DESC
    LIMIT 2;
    """
    run_query(conn, query_8, "8. Топ-2 найдорожчі послуги")

    # 9. Кількість записів на кожного майстра.
    query_9 = """
    SELECT m.name AS Master, COUNT(a.id) AS total_appointments
    FROM masters m
    LEFT JOIN appointments a ON a.master_id = m.id
    GROUP BY m.id
    ORDER BY total_appointments DESC;
    """
    run_query(conn, query_9, "9. Кількість записів на кожного майстра")

    # 10. Загальний дохід (сума цін) від 'completed' записів.
    query_10 = """
    SELECT SUM(s.price) AS total_revenue
    FROM appointments a
    JOIN services s ON a.service_id = s.id
    WHERE a.status = 'completed';
    """
    run_query(conn, query_10, "10. Загальний дохід від завершених записів")

    # 11. Майстри, які ще не мають жодного запису.
    query_11 = """
    SELECT m.name
    FROM masters m
    LEFT JOIN appointments a ON a.master_id = m.id
    WHERE a.id IS NULL;
    """
    run_query(conn, query_11, "11. Майстри без записів")

    # 12. Послуги, які були заброньовані понад 2 рази.
    query_12 = """
    SELECT s.name AS Service, COUNT(a.id) AS booking_count
    FROM services s
    JOIN appointments a ON a.service_id = s.id
    GROUP BY s.id
    HAVING COUNT(a.id) > 1
    ORDER BY booking_count DESC;
    """
    run_query(conn, query_12, "12. Послуги, заброньовані > 1 раз")

    # 13. Показати: Клієнт, Майстер, Послуга та Ціна (один рядок на запис).
    query_13 = """
    SELECT 
        c.full_name AS Client, 
        m.name AS Master, 
        s.name AS Service, 
        s.price
    FROM appointments a
    JOIN clients c ON a.client_id = c.id
    JOIN masters m ON a.master_id = m.id
    JOIN services s ON a.service_id = s.id
    ORDER BY Client;
    """
    run_query(conn, query_13, "13. Зведена таблиця: Клієнт-Майстер-Послуга-Ціна")

    # 14. Майстри, які працюють 'Стиліст' (витягнути з поля specialization).
    query_14 = """
    SELECT name, specialization
    FROM masters
    WHERE specialization = 'Стиліст';
    """
    run_query(conn, query_14, "14. Майстри зі спеціалізацією 'Стиліст'")

    # 15. Записи, створені клієнтом 'Вікторія Семенко' у грудні 2025 року.
    query_15 = """
    SELECT a.date, a.status
    FROM appointments a
    JOIN clients c ON c.id = a.client_id
    WHERE c.full_name = 'Вікторія Семенко' 
      AND a.date LIKE '2025-12%';
    """
    run_query(conn, query_15, "15. Записи Вікторії у грудні 2025")

    # 16. Додати нового клієнта.
    insert_client_sql = """
    INSERT INTO clients (full_name, email, phone_number) 
    VALUES ('Новий Клієнт', 'new.client@test.com', '0990001122');
    """
    execute_script(conn, insert_client_sql)
    print("\n--- 16. Додано нового клієнта. ---")

    # 17. Додати новий запис для цього клієнта на послугу 'Манікюр класичний'.
    insert_appointment_sql = f"""
    INSERT INTO appointments (date, status, master_id, client_id, service_id)
    SELECT 
        '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}', 
        'booked', 
        m.id, 
        c.id, 
        s.id
    FROM masters m, clients c, services s
    WHERE m.specialization = 'Манікюр' AND c.full_name = 'Новий Клієнт' AND s.name = 'Манікюр класичний'
    LIMIT 1;
    """
    execute_script(conn, insert_appointment_sql)
    print("--- 17. Додано новий запис для 'Новий Клієнт'. ---")

    # 18. Оновити статус запису на 'completed' для клієнта 'Анна Петренко' на 2025-12-05.
    update_appointment_sql = """
    UPDATE appointments
    SET status = 'completed'
    WHERE date LIKE '2025-12-05 15:00' 
      AND client_id = (SELECT id FROM clients WHERE full_name = 'Анна Петренко');
    """
    execute_script(conn, update_appointment_sql)
    print("--- 18. Оновлено статус запису Анни на 'completed'. ---")

    # 19. Підвищити ціну на всі послуги 'Стрижка' на 50 грн.
    update_price_sql = """
    UPDATE services
    SET price = price + 50.00
    WHERE name LIKE '%стрижка%';
    """
    execute_script(conn, update_price_sql)
    print("--- 19. Підвищено ціну на всі стрижки на 50 грн. ---")

    # 20. Видалити всі скасовані (canceled) записи до сьогоднішньої дати.
    delete_canceled_sql = f"""
    DELETE FROM appointments
    WHERE status = 'canceled' AND date(date) <= date('{datetime.date.today().strftime('%Y-%m-%d')}');
    """
    execute_script(conn, delete_canceled_sql)
    print("--- 20. Видалено скасовані записи до сьогодні. ---")

    run_query(conn, query_1, "Перевірка 1 (після UPDATE/INSERT/DELETE)")

    conn.close()
    print("Роботу завершено. Файл бази даних 'salon_database.db' оновлено.")


if __name__ == "__main__":
    main()