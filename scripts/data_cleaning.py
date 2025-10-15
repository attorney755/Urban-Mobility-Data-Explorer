# Attorney Valois NIYIGABA: I created this script to clean and preprocess the NYC taxi trip dataset.
# I focused on removing invalid, missing, or duplicate data, and ensuring all coordinates fall within NYC bounds.
# I also derived new features and saved both cleaned data and excluded records for transparency.

import pandas as pd

# I loaded the raw dataset from the specified path.
raw_data = pd.read_csv('../data/raw/train.csv')

# I printed an overview of the raw data to understand its structure and content.
print("=== Raw Data Overview ===")
print("Shape:", raw_data.shape)
print("Columns:", raw_data.columns.tolist())
print("\n=== Sample Data ===")
print(raw_data.head())

# I defined critical columns that must not have missing values for the analysis.
critical_columns = [
    'pickup_datetime', 'dropoff_datetime',
    'pickup_longitude', 'pickup_latitude',
    'dropoff_longitude', 'dropoff_latitude',
    'trip_duration'
]

# I removed rows with missing values in critical columns to ensure data quality.
cleaned_data = raw_data.dropna(subset=critical_columns)

# I removed duplicate rows to avoid redundancy in the dataset.
cleaned_data = cleaned_data.drop_duplicates()

# I converted timestamp columns to datetime objects for easier manipulation.
cleaned_data['pickup_datetime'] = pd.to_datetime(cleaned_data['pickup_datetime'])
cleaned_data['dropoff_datetime'] = pd.to_datetime(cleaned_data['dropoff_datetime'])

# I removed rows where the dropoff time is before the pickup time, as these are invalid.
cleaned_data = cleaned_data[cleaned_data['dropoff_datetime'] >= cleaned_data['pickup_datetime']]

# I defined the geographic bounds of New York City to filter out invalid coordinates.
nyc_bounds = {
    'min_lat': 40.4774, 'max_lat': 40.9176,
    'min_lon': -74.2591, 'max_lon': -73.7004
}

# I filtered the data to only include coordinates within NYC bounds.
cleaned_data = cleaned_data[
    (cleaned_data['pickup_latitude'] >= nyc_bounds['min_lat']) &
    (cleaned_data['pickup_latitude'] <= nyc_bounds['max_lat']) &
    (cleaned_data['pickup_longitude'] >= nyc_bounds['min_lon']) &
    (cleaned_data['pickup_longitude'] <= nyc_bounds['max_lon']) &
    (cleaned_data['dropoff_latitude'] >= nyc_bounds['min_lat']) &
    (cleaned_data['dropoff_latitude'] <= nyc_bounds['max_lat']) &
    (cleaned_data['dropoff_longitude'] >= nyc_bounds['min_lon']) &
    (cleaned_data['dropoff_longitude'] <= nyc_bounds['max_lon'])
]

# I rounded coordinate values to 6 decimal places for precision and consistency.
coordinate_columns = [
    'pickup_longitude', 'pickup_latitude',
    'dropoff_longitude', 'dropoff_latitude'
]
for col in coordinate_columns:
    cleaned_data[col] = cleaned_data[col].round(6)

# I derived a new feature: trip_duration_min, converting trip duration from seconds to minutes.
cleaned_data['trip_duration_min'] = cleaned_data['trip_duration'] / 60

# I logged excluded records to a separate file for review and transparency.
excluded_records = raw_data[~raw_data.index.isin(cleaned_data.index)]
excluded_records.to_csv('../data/processed/excluded_records.csv', index=False)

# I saved the cleaned data to a new CSV file for further analysis.
cleaned_data.to_csv('../data/processed/cleaned_trips.csv', index=False)

# I printed an overview of the cleaned data to verify the cleaning process.
print("\n=== Cleaned Data Overview ===")
print("Shape:", cleaned_data.shape)
print("Columns:", cleaned_data.columns.tolist())
print("\n=== Sample Cleaned Data ===")
print(cleaned_data.head())
