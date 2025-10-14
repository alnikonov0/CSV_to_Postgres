import pandas as pd
import numpy as np

def generate_test_data(n_rows: int, output_path: str = "test_etl_data.csv"):
    """
    Генерация тестового CSV файла для ETL-практики.

    :param n_rows: количество строк
    :param output_path: путь к сохраняемому CSV
    """
    np.random.seed(42)
    first_names = ["Иван", "Петр", "Сергей", "Анна", "Ольга", "Мария", "Дмитрий", "Алексей"]
    last_names = ["Иванов", "Петров", "Сидоров", "Смирнова", "Кузнецова", "Попова", "Соколова", "Морозов"]
    products = ["Телефон", "Ноутбук", "Планшет", "Наушники", "Телевизор", "Часы", "Клавиатура", "Мышь"]
    genders = ["М", "Ж"]

    # Генерация массивов (работает быстрее, чем списки для миллионов строк)
    fio = np.char.add(np.random.choice(first_names, n_rows), " ")
    fio = np.char.add(fio, np.random.choice(last_names, n_rows))

    df = pd.DataFrame({
        "fio": fio,
        "gender": np.random.choice(genders, size=n_rows),
        "age": np.random.randint(18, 70, size=n_rows),
        "product": np.random.choice(products, size=n_rows),
        "quantity": np.random.randint(1, 5, size=n_rows)
    })

    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"✅ Файл сохранён: {output_path}, строк: {n_rows}")


if __name__ == "__main__":
    # Пример использования:
    generate_test_data(1_000_000, "test_etl_data_1m.csv")   # 1 млн строк
    # generate_test_data(5_000_000, "test_etl_data_5m.csv") # 5 млн строк
    # generate_test_data(10_000_000, "test_etl_data_10m.csv") # 10 млн строк
