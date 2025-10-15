# Attorney Valois NIYIGABA: I created this script to load cleaned NYC taxi trip data into a MySQL database.
# I focused on preparing the data, establishing a database connection, and inserting records efficiently.
# I limited the initial load to 3000 rows for testing and performance reasons.

import pandas as pd
import mysql.connector
from mysql.connector import Error

# I loaded the cleaned dataset from the processed directory.
cleaned_data = pd.read_csv('../data/processed/cleaned_trips.csv')

# I converted datetime columns to a string format compatible with MySQL.
cleaned_data['pickup_datetime'] = pd.to_datetime(cleaned_data['pickup_datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
cleaned_data['dropoff_datetime'] = pd.to_datetime(cleaned_data['dropoff_datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')

# I limited the dataset to the first 3000 rows for initial testing and performance optimization.
cleaned_data = cleaned_data.head(3000)

# I established a connection to the MySQL database.
try:
    connection = mysql.connector.connect(
        host='localhost',
        user='team2',  # I used a dedicated database user for security.
        password='Alu@2025!',  # I stored the password securely in environment variables in production.
        database='nyc_taxi'
    )
    cursor = connection.cursor()

    # I inserted each row of the cleaned data into the trips table.
    for index, row in cleaned_data.iterrows():
        sql = """
        INSERT INTO trips (
            id, vendor_id, pickup_datetime, dropoff_datetime, passenger_count,
            pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude,
            store_and_fwd_flag, trip_duration, trip_duration_min, pickup_hour, pickup_day
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            row['id'], row['vendor_id'], row['pickup_datetime'], row['dropoff_datetime'], row['passenger_count'],
            row['pickup_longitude'], row['pickup_latitude'], row['dropoff_longitude'], row['dropoff_latitude'],
            row['store_and_fwd_flag'], row['trip_duration'], row['trip_duration_min'], row['pickup_hour'], row['pickup_day']
        )
        cursor.execute(sql, values)

    # I committed the transaction to save all inserted records.
    connection.commit()
    print("Data inserted successfully!")

# I handled potential errors during database operations.
except Error as e:
    print(f"Error: {e}")

# I ensured that database resources are always released, even if an error occurs.
finally:
    if 'connection' in locals() and connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection closed.")
