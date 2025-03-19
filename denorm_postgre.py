import psycopg2
import pandas as pd
import numpy as np
import time
# 1. Funkcja do nawiązania połączenia z bazą danych
def connect_to_db():
    try:
        connection = psycopg2.connect("dbname='denorm' user='postgres' host='192.168.56.103' port='5432' password='pracamgr'")
        return connection
    except psycopg2.Error as e:
        print(f"Błąd podczas łączenia z bazą danych: {e}")
        return None

# 2. Wczytanie danych z pliku CSV
def load_csv_data(file_path):
    try:
        data = pd.read_csv(file_path)
        print(f"Wczytano {len(data)} wierszy z pliku {file_path}")
        return data
    except Exception as e:
        print(f"Błąd podczas wczytywania pliku CSV: {e}")
        return None

# 3. Wstawienie danych do bazy danych
def insert_data_to_db(connection, table_name, data):
    try:
        cursor = connection.cursor()

        # Ręczne określenie kolumn z CSV, które będą używane
        selected_columns_csv = ['year','make','model','body','transmission','vin','state','condition','odometer','color','seller','mmr','sellingprice','saledate']  # Kolumny z CSV
        selected_columns_db = "year, make, model, body, transmission, vin, state, car_condition, odometer, color, seller, mmr, selling_price, sale_date"  # Kolumny w tabeli w bazie danych

        # Usuwamy strefę czasową i pozostawiamy tylko datę i godzinę
        data['saledate'] = data['saledate'].str.replace(r' GMT[^\s]* \([^\)]*\)', '', regex=True)

        # Konwersja daty na datetime (tylko data i godzina)
        data['saledate'] = pd.to_datetime(
            data['saledate'],
            format='%a %b %d %Y %H:%M:%S',  # Format: 'Tue Mar 03 2015 05:00:00'
            utc=False
        )

        # Formatowanie daty do standardu YYYY-MM-DD HH:MM:SS
        data['saledate'] = data['saledate'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Wstawiamy dokładnie 'num_rows' wierszy (np. pierwsze 50 tysięcy)
        data_to_insert = data[selected_columns_csv]
        
        rows_to_insert = []
        for _, x in data_to_insert.iterrows():
        # Dobieramy tylko wartości z tych kolumn, które chcemy wstawić
            row_data = tuple(
            [
            int(x[col]) if isinstance(x[col], np.int64) else
            float(x[col]) if isinstance(x[col], np.float64) else
            str(x[col]) if isinstance(x[col], str) else
            x[col]
            for col in selected_columns_csv
            ]
            )
            rows_to_insert.append(row_data)
        
        # Przygotowanie zapytania SQL
        placeholders = ", ".join(["%s"] * len(selected_columns_csv))
        sql_query = f"INSERT INTO {table_name} ({selected_columns_db}) VALUES ({placeholders})"

        # Wstawianie danych
        start_time = time.time()
        cursor.executemany(sql_query, rows_to_insert)
        connection.commit()
        end_time = time.time()
        elapsed_time = end_time - start_time 
        print(f"Pomyślnie wstawiono {len(rows_to_insert)} wierszy do tabeli {table_name}")
        print(f"Czas wstawiania danych: {elapsed_time:.2f} sekundy")
    except psycopg2.Error as e:
        print(f"Błąd podczas wstawiania danych: {e}")
        connection.rollback()
    finally:
        cursor.close()

# 4. Główna funkcja programu
def main():
    # Ścieżka do pliku CSV
    csv_file_path = r"C:\Users\abrac\Desktop\cars.csv"
    
    # Nazwa tabeli w bazie danych
    table_name = "car_sale_info"

    # Nawiązanie połączenia z bazą danych
    connection = connect_to_db()
    if not connection:
        return
    print("Połączono z bazą danych PostgreSQL")

        # Wczytanie danych z pliku CSV
    data = load_csv_data(csv_file_path)
    if data is None:
        return
    
    # Wstawienie danych do bazy danych
    insert_data_to_db(connection, table_name, data)
    
    # Zamknięcie połączenia
    connection.close()

if __name__ == "__main__":
    main()