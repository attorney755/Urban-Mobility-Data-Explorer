# scripts/db_setup.py
import pandas as pd
import mysql.connector
from mysql.connector import Error

# Load the cleaned data
cleaned_data = pd.read_csv('../data/processed/cleaned_trips.csv')

# Convert pickup_datetime and dropoff_datetime to string format for MySQL
cleaned_data['pickup_datetime'] = pd.to_datetime(cleaned_data['pickup_datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
cleaned_data['dropoff_datetime'] = pd.to_datetime(cleaned_data['dropoff_datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')

# Limit to first 5000 rows
cleaned_data = cleaned_data.head(3000)

# Connect to MySQL
try:
    connection = mysql.connector.connect(
        host='localhost',
        user='team2',  # Replace with your MySQL username
        password='Alu@2025!',  # Replace with your MySQL password
        database='nyc_taxi'
    )

    cursor = connection.cursor()

    # Insert data into the trips table
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

    # Commit the transaction
    connection.commit()
    print("Data inserted successfully!")

except Error as e:
    print(f"Error: {e}")

finally:
    if 'connection' in locals() and connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection closed.")
