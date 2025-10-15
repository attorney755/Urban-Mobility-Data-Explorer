# Attorney Valois NIYIGABA: I created a Flask backend to serve NYC taxi trip data from a MySQL database.
# I used Flask-CORS to allow cross-origin requests and structured the code for clarity and maintainability.

from flask import Flask, jsonify, request
import mysql.connector
from mysql.connector import Error
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # I enabled CORS to allow frontend-backend communication during development.

def get_db_connection():
    """
    I created a helper function to establish a connection to the MySQL database.
    This function handles connection errors and returns the connection object.
    """
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='team2', 
            password='Alu@2025!', 
            database='nyc_taxi'
        )
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

@app.route('/trips', methods=['GET'])
def get_trips():
    """
    I implemented an endpoint to fetch trips with optional query parameters for filtering.
    This allows the frontend to request only the data it needs, improving performance.
    """
    # I extracted query parameters for filtering trips.
    min_duration = request.args.get('min_duration', default=0, type=float)
    max_duration = request.args.get('max_duration', default=1000, type=float)
    pickup_hour = request.args.get('pickup_hour', default=None, type=int)
    pickup_day = request.args.get('pickup_day', default=None, type=str)

    # I built a dynamic SQL query based on the provided filters.
    query = "SELECT * FROM trips WHERE 1=1"
    params = []

    if min_duration > 0:
        query += " AND trip_duration_min >= %s"
        params.append(min_duration)

    if max_duration < 1000:
        query += " AND trip_duration_min <= %s"
        params.append(max_duration)

    if pickup_hour is not None:
        query += " AND pickup_hour = %s"
        params.append(pickup_hour)

    if pickup_day is not None:
        query += " AND pickup_day = %s"
        params.append(pickup_day)

    # I connected to the database and executed the query.
    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection failed"}), 500

    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, params)
        trips = cursor.fetchall()
        return jsonify(trips)
    except Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        # I ensured that database resources are always released.
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == '__main__':
    # I configured the app to run in debug mode for development.
    app.run(debug=True, host='0.0.0.0', port=5000)
