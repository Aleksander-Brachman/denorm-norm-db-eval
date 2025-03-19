import mysql.connector
import pandas as pd
import numpy as np
import time
# 1. Funkcja do nawiązania połączenia z bazą danych
def connect_to_db():
    try:
        connection = mysql.connector.connect(
            host="192.168.56.103",
            database="norm", 
            user="admin", 
            password="pracamgr"         
        )
        return connection
    except mysql.connector.Error as e:
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
def insert_data_to_db(connection, data_body, data_color, data_seller, data):
    try:
        cursor = connection.cursor()
       
        placeholders_bdclsel = ", ".join(["%s"] * 2)
        
        data_body_to_insert = data_body
        rows_body = [tuple(x) for x in data_body_to_insert.values]
        
        data_color_to_insert = data_color
        rows_color = [tuple(x) for x in data_color_to_insert.values]

        data_seller_to_insert = data_seller
        rows_seller = [tuple(x) for x in data_seller_to_insert.values]


        # Ręczne określenie kolumn z CSV, które będą używane
        selected_columns_csv_car = ['year','make','model','vin','condition','odometer','color','body','transmission']
        selected_columns_db_car = "year, make, model, vin, car_condition, odometer, color_id, body_id, transmission_id"

        selected_columns_csv_sale = ['vin','mmr','sellingprice', 'saledate','seller', 'state']
        selected_columns_db_sale = "vin, mmr, selling_price, sale_date, seller_id"

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
        data_to_insert_car = data[selected_columns_csv_car]
        data_to_insert_sale = data[selected_columns_csv_sale]
        rows_to_insert_car = []
        for _, x in data_to_insert_car.iterrows():
        # Dobieramy tylko wartości z tych kolumn, które chcemy wstawić
            row_data_car = tuple(
            [
            int(x[col]) if isinstance(x[col], np.int64) else
            float(x[col]) if isinstance(x[col], np.float64) else
            str(x[col]) if isinstance(x[col], str) else
            x[col]
            for col in selected_columns_csv_car
            ]
            )
            rows_to_insert_car.append(row_data_car)

        rows_to_insert_sale = []
        for _, x in data_to_insert_sale.iterrows():
        # Dobieramy tylko wartości z tych kolumn, które chcemy wstawić
            row_data_sale = tuple(
            [
            int(x[col]) if isinstance(x[col], np.int64) else
            float(x[col]) if isinstance(x[col], np.float64) else
            str(x[col]) if isinstance(x[col], str) else
            x[col]
            for col in selected_columns_csv_sale
            ]
            )
            rows_to_insert_sale.append(row_data_sale)
        
        # Przygotowanie zapytania SQL
        placeholders_car = f"%s, %s, %s, %s, %s, %s, (SELECT color_id FROM color_info WHERE color = %s), (SELECT body_id FROM body_info WHERE body = %s), (SELECT transmission_id FROM transmission_info WHERE transmission = %s)"
        placeholders_sale = f"%s, %s, %s, %s, (SELECT seller_id FROM seller_info WHERE seller = %s AND state = %s)"

        sql_query_transm = "INSERT INTO transmission_info (transmission_id, transmission) VALUES ('a', 'automatic'), ('m', 'manual')"
        sql_query_color = f"INSERT INTO color_info (color_id, color) values ({placeholders_bdclsel})"
        sql_query_body = f"INSERT INTO body_info (body_id, body) values ({placeholders_bdclsel})"
        sql_query_car = f"INSERT INTO car_details ({selected_columns_db_car}) VALUES ({placeholders_car})"
        sql_query_seller = f"INSERT INTO seller_info (seller, state) values ({placeholders_bdclsel})"
        sql_query_sale = f"INSERT INTO sale_details ({selected_columns_db_sale}) VALUES ({placeholders_sale})"
        
        # Wstawianie danych
        batch_size = 1000
        start_time = time.time()
        cursor.execute(sql_query_transm)
        cursor.executemany(sql_query_color, rows_color)
        cursor.executemany(sql_query_body, rows_body)
        
        for i in range(0, len(rows_to_insert_car), batch_size):
            batch = rows_to_insert_car[i:i + batch_size]
            cursor.executemany(sql_query_car, batch)

        print("Wstawiono dane do tabeli car_details")    
        cursor.executemany(sql_query_seller, rows_seller) 

        for i in range(0, len(rows_to_insert_sale), batch_size):
            batch = rows_to_insert_sale[i:i + batch_size]
            cursor.executemany(sql_query_sale, batch)
        connection.commit()
        end_time = time.time()
        elapsed_time = end_time - start_time 
        print(f"Pomyślnie wstawiono dane do wszystkich tabel")
        print(f"Czas wstawiania danych: {elapsed_time:.2f} sekundy")
    except mysql.connector.Error as e:
        print(f"Błąd podczas wstawiania danych: {e}")
        connection.rollback()
    finally:
        cursor.close()

# 4. Główna funkcja programu
def main():
    # Ścieżka do pliku CSV
    csv_file_path_color = r"C:\Users\abrac\Desktop\colors.csv"
    csv_file_path_body = r"C:\Users\abrac\Desktop\body.csv"
    csv_file_path_seller = r"C:\Users\abrac\Desktop\sellers.csv"
    csv_file_path = r"C:\Users\abrac\Desktop\cars.csv"
    
    # Nawiązanie połączenia z bazą danych
    connection = connect_to_db()
    
    if not connection:
        return
    print("Połączono z bazą danych MariaDB")
    
    # Wczytanie danych z pliku CSV
    data_color = load_csv_data(csv_file_path_color)
    if data_color is None:
        return
    data_body = load_csv_data(csv_file_path_body)
    if data_body is None:
        return
    data_seller = load_csv_data(csv_file_path_seller)
    if data_seller is None:
        return
    data = load_csv_data(csv_file_path)
    if data is None:
        return
    
    # Wstawienie danych do bazy danych
    insert_data_to_db(connection, data_body, data_color, data_seller, data)
    
    # Zamknięcie połączenia
    connection.close()

if __name__ == "__main__":
    main()