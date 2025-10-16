# Attorney Valois NIYIGABA: I created this script to clean and preprocess NYC taxi trip data.
# I implemented custom functions for datetime parsing, coordinate validation, CSV reading/writing, and data filtering.
# The script ensures data quality by removing invalid records and deriving new features.

# I created a custom function to parse datetime strings into a structured tuple.
def parse_datetime(datetime_str):
    try:
        # I split the datetime string into date and time components.
        date_part, time_part = datetime_str.split(' ')
        year, month, day = map(int, date_part.split('-'))
        hour, minute, second = map(int, time_part.split(':'))
        # I added basic validation to ensure the datetime values are reasonable.
        if (1 <= month <= 12 and 1 <= day <= 31 and
            0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
            return (year, month, day, hour, minute, second)
        return None
    except:
        return None

# I created a custom function to compare two datetime tuples.
def compare_datetimes(dt1, dt2):
    if not dt1 or not dt2:
        return False
    # I implemented a step-by-step comparison to ensure pickup time is before dropoff time.
    return (dt1[0] < dt2[0] or
            (dt1[0] == dt2[0] and dt1[1] < dt2[1]) or
            (dt1[0] == dt2[0] and dt1[1] == dt2[1] and dt1[2] < dt2[2]) or
            (dt1[0] == dt2[0] and dt1[1] == dt2[1] and dt1[2] == dt2[2] and dt1[3] < dt2[3]) or
            (dt1[0] == dt2[0] and dt1[1] == dt2[1] and dt1[2] == dt2[2] and dt1[3] == dt2[3] and dt1[4] < dt2[4]) or
            (dt1[0] == dt2[0] and dt1[1] == dt2[1] and dt1[2] == dt2[2] and dt1[3] == dt2[3] and dt1[4] == dt2[4] and dt1[5] <= dt2[5]))

# I created a custom function to validate if coordinates fall within NYC bounds.
def is_valid_coordinate(lat, lon, nyc_bounds):
    try:
        lat = float(lat)
        lon = float(lon)
        return (nyc_bounds['min_lat'] <= lat <= nyc_bounds['max_lat'] and
                nyc_bounds['min_lon'] <= lon <= nyc_bounds['max_lon'])
    except:
        return False

# I created a custom function to read CSV files without using pandas.
def read_csv(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
    headers = lines[0].strip().split(',')
    rows = []
    for line in lines[1:]:
        values = line.strip().split(',')
        row = {}
        for i, header in enumerate(headers):
            row[header] = values[i] if i < len(values) else ''
        rows.append(row)
    return headers, rows

# I created a custom function to write CSV files without using pandas.
def write_csv(file_path, headers, rows):
    with open(file_path, 'w', encoding='utf-8', newline='') as file:
        file.write(','.join(headers) + '\n')
        for row in rows:
            line = []
            for header in headers:
                line.append(str(row.get(header, '')))
            file.write(','.join(line) + '\n')

# I created a custom function to clean and filter the data.
def clean_data(input_file, output_file, excluded_file):
    # I defined the geographic bounds of New York City.
    nyc_bounds = {
        'min_lat': 40.4774, 'max_lat': 40.9176,
        'min_lon': -74.2591, 'max_lon': -73.7004
    }
    headers, rows = read_csv(input_file)
    cleaned_rows = []
    excluded_rows = []
    for row in rows:
        # I parsed the pickup and dropoff timestamps.
        pickup_datetime = parse_datetime(row['pickup_datetime'])
        dropoff_datetime = parse_datetime(row['dropoff_datetime'])

        # I checked for missing values in critical fields.
        critical_fields = [
            'pickup_datetime', 'dropoff_datetime',
            'pickup_longitude', 'pickup_latitude',
            'dropoff_longitude', 'dropoff_latitude',
            'trip_duration'
        ]
        missing_fields = [field for field in critical_fields if not row.get(field)]
        if missing_fields:
            excluded_rows.append(row)
            continue

        # I checked for invalid timestamps where dropoff is before pickup.
        if not compare_datetimes(pickup_datetime, dropoff_datetime):
            excluded_rows.append(row)
            continue

        # I parsed the coordinates and checked for errors.
        try:
            pickup_lat = row['pickup_latitude']
            pickup_lon = row['pickup_longitude']
            dropoff_lat = row['dropoff_latitude']
            dropoff_lon = row['dropoff_longitude']
        except:
            excluded_rows.append(row)
            continue

        # I checked if coordinates are within NYC bounds.
        if (not is_valid_coordinate(pickup_lat, pickup_lon, nyc_bounds) or
            not is_valid_coordinate(dropoff_lat, dropoff_lon, nyc_bounds)):
            excluded_rows.append(row)
            continue

        # I rounded coordinates to 6 decimal places for consistency.
        try:
            row['pickup_longitude'] = str(round(float(row['pickup_longitude']), 6))
            row['pickup_latitude'] = str(round(float(row['pickup_latitude']), 6))
            row['dropoff_longitude'] = str(round(float(row['dropoff_longitude']), 6))
            row['dropoff_latitude'] = str(round(float(row['dropoff_latitude']), 6))
        except:
            excluded_rows.append(row)
            continue

        # I added a derived feature: trip_duration_min (converting seconds to minutes).
        try:
            row['trip_duration_min'] = str(float(row['trip_duration']) / 60)
        except:
            excluded_rows.append(row)
            continue

        cleaned_rows.append(row)

    # I added the new header for the derived feature if it doesn't exist.
    if 'trip_duration_min' not in headers:
        headers = headers + ['trip_duration_min']

    # I wrote the cleaned data to the output file.
    write_csv(output_file, headers, cleaned_rows)

    # I wrote the excluded records to a separate file for review.
    write_csv(excluded_file, headers, excluded_rows)

# I defined the input and output file paths.
input_file = '../data/raw/train.csv'
output_file = '../data/processed/cleaned_trips.csv'
excluded_file = '../data/processed/excluded_records.csv'

# I executed the data cleaning process.
print("=== Starting Data Cleaning ===")
clean_data(input_file, output_file, excluded_file)
print("=== Data Cleaning Complete ===")
