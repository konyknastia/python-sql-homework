import sqlite3

# Назва файлу бази даних
DB_NAME = "hw_database.db"


def create_connection():
    """Створює підключення до БД і вмикає Foreign Keys"""
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def execute_script(conn, script):
    """Виконує SQL-скрипт (кілька команд через ;)"""
    cursor = conn.cursor()
    cursor.executescript(script)
    conn.commit()


def run_query(conn, query, title="Результат запиту"):
    """Виконує SELECT запит і гарно друкує результат"""
    cursor = conn.cursor()
    cursor.execute(query)

    # Отримуємо назви колонок
    columns = [description[0] for description in cursor.description]
    rows = cursor.fetchall()

    print(f"\n--- {title} ---")
    # Друкуємо заголовок (назви колонок)
    print(f"{' | '.join(columns)}")
    print("-" * 40)

    # Друкуємо рядки
    if not rows:
        print("(Немає даних)")
    else:
        for row in rows:
            # Перетворюємо всі елементи в стрічку для друку
            print(" | ".join(str(item) for item in row))
    print("-" * 40)


def main():
    # 1. СТВОРЕННЯ БАЗИ ДАНИХ (Д/З-1)
    # ---------------------------------------------------------
    conn = create_connection()
    print("1. База даних підключена.")

    # SQL для очищення (щоб скрипт можна було запускати багаторазово)
    drop_tables_sql = """
    DROP TABLE IF EXISTS tickets;
    DROP TABLE IF EXISTS employee_projects;
    DROP TABLE IF EXISTS projects;
    DROP TABLE IF EXISTS employees;
    DROP TABLE IF EXISTS clients;
    DROP TABLE IF EXISTS roles;
    DROP TABLE IF EXISTS departments;
    """

    # SQL для створення таблиць (Схема з попередніх завдань)
    create_tables_sql = """
    CREATE TABLE departments (
        dept_id INTEGER PRIMARY KEY,
        name    TEXT NOT NULL UNIQUE
    );

    CREATE TABLE roles (
        role_id INTEGER PRIMARY KEY,
        name    TEXT NOT NULL UNIQUE
    );

    CREATE TABLE clients (
        client_id INTEGER PRIMARY KEY,
        name      TEXT NOT NULL,
        country   TEXT
    );

    CREATE TABLE employees (
        emp_id     INTEGER PRIMARY KEY,
        full_name  TEXT NOT NULL,
        email      TEXT UNIQUE,
        hire_date  TEXT NOT NULL,
        dept_id    INTEGER,
        role_id    INTEGER,
        salary_usd REAL NOT NULL CHECK (salary_usd > 0),
        FOREIGN KEY (dept_id) REFERENCES departments(dept_id),
        FOREIGN KEY (role_id) REFERENCES roles(role_id)
    );

    CREATE TABLE projects (
        project_id  INTEGER PRIMARY KEY,
        name        TEXT NOT NULL,
        client_id   INTEGER,
        start_date  TEXT NOT NULL,
        due_date    TEXT,
        budget_usd  REAL,
        status      TEXT CHECK (status IN ('planned', 'active', 'on-hold', 'done')),
        FOREIGN KEY (client_id) REFERENCES clients(client_id)
    );

    CREATE TABLE employee_projects (
        emp_id      INTEGER,
        project_id  INTEGER,
        assigned_at TEXT NOT NULL,
        PRIMARY KEY (emp_id, project_id),
        FOREIGN KEY (emp_id) REFERENCES employees(emp_id),
        FOREIGN KEY (project_id) REFERENCES projects(project_id)
    );

    CREATE TABLE tickets (
        ticket_id  INTEGER PRIMARY KEY,
        project_id INTEGER,
        title      TEXT NOT NULL,
        created_at TEXT NOT NULL,
        status     TEXT CHECK (status IN ('open', 'in-progress', 'closed')) DEFAULT 'open',
        FOREIGN KEY (project_id) REFERENCES projects(project_id)
    );
    """

    print("   Видалення старих таблиць та створення нових...")
    execute_script(conn, drop_tables_sql)
    execute_script(conn, create_tables_sql)

    # 2. НАПОВНЕННЯ ДАНИМИ (INSERT)
    # ---------------------------------------------------------
    print("2. Наповнення бази даними...")
    insert_data_sql = """
    INSERT INTO departments (name) VALUES ('R&D'), ('QA'), ('Sales');
    INSERT INTO roles (name) VALUES ('Developer'), ('QA Engineer'), ('Manager');
    INSERT INTO clients (name, country) VALUES ('Acme Corp', 'USA'), ('Sakura', 'Japan');

    INSERT INTO employees (full_name, email, hire_date, dept_id, role_id, salary_usd) VALUES
    ('Ivan Petrenko', 'ivan@itco.com', '2023-01-10', 1, 1, 4000),
    ('Maria Koval', 'maria@itco.com', '2024-05-15', 2, 2, 3500),
    ('Oleg Bondar', 'oleg@itco.com', '2022-08-20', 1, 1, 4200),
    ('Anna Svit', 'anna@itco.com', '2021-11-01', 3, 3, 5000);

    INSERT INTO projects (name, client_id, start_date, due_date, budget_usd, status) VALUES
    ('Super App', 1, '2024-01-01', '2025-12-31', 100000, 'active'),
    ('Legacy System', 1, '2022-01-01', '2023-01-01', 50000, 'done'),
    ('New Website', 2, '2025-02-01', '2025-06-01', 20000, 'planned');

    INSERT INTO employee_projects (emp_id, project_id, assigned_at) VALUES
    (1, 1, '2024-01-05'),
    (2, 1, '2024-02-01'),
    (3, 2, '2022-01-05');

    INSERT INTO tickets (project_id, title, created_at, status) VALUES
    (1, 'Fix login bug', '2025-03-01', 'open'),
    (1, 'Update logo', '2025-03-05', 'in-progress'),
    (2, 'Cleanup database', '2022-10-10', 'closed');
    """
    execute_script(conn, insert_data_sql)

    # 3. ВИКОНАННЯ SELECT ЗАПИТІВ (Д/З-2)
    # ---------------------------------------------------------
    print("3. Виконання SELECT запитів...")

    # Запит 3.1: Показати працівників, їх відділ та зарплату
    query_employees = """
    SELECT e.full_name, d.name as dept, e.salary_usd
    FROM employees e
    JOIN departments d ON e.dept_id = d.dept_id;
    """
    run_query(conn, query_employees, "Список працівників і зарплат")

    # Запит 3.2: Активні проєкти
    query_projects = """
    SELECT name, budget_usd, status FROM projects WHERE status = 'active';
    """
    run_query(conn, query_projects, "Активні проєкти")

    # 4. ОНОВЛЕННЯ ЗАПИСІВ (UPDATE)
    # ---------------------------------------------------------
    print("4. Оновлення записів (UPDATE)...")

    # Підвищимо зарплату всім у відділі R&D (dept_id = 1) на 500
    update_salary_sql = """
    UPDATE employees 
    SET salary_usd = salary_usd + 500 
    WHERE dept_id = (SELECT dept_id FROM departments WHERE name = 'R&D');
    """
    cursor = conn.cursor()
    cursor.execute(update_salary_sql)
    conn.commit()
    print(f"   Оновлено записів: {cursor.rowcount}")

    # Закриємо всі тікети по проєкту 'Super App'
    update_tickets_sql = """
    UPDATE tickets
    SET status = 'closed'
    WHERE project_id = (SELECT project_id FROM projects WHERE name = 'Super App');
    """
    cursor.execute(update_tickets_sql)
    conn.commit()
    print(f"   Закрито тікетів: {cursor.rowcount}")

    # 5. ПЕРЕВІРКА ЗМІН (SELECT ПІСЛЯ UPDATE)
    # ---------------------------------------------------------
    print("5. Перевірка результатів після оновлення...")

    # Перевіряємо зарплату (Ivan Petrenko мав 4000, Oleg Bondar мав 4200. Обидва з R&D)
    run_query(conn, query_employees, "Оновлені зарплати (R&D +500)")

    # Перевіряємо статуси тікетів
    query_tickets = """
    SELECT t.title, p.name as project, t.status
    FROM tickets t
    JOIN projects p ON t.project_id = p.project_id
    WHERE p.name = 'Super App';
    """
    run_query(conn, query_tickets, "Тікети Super App (мають бути closed)")

    conn.close()
    print("Роботу завершено. З'єднання закрито.")


if __name__ == "__main__":
    main()