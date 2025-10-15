# scripts/data_cleaning.py
import pandas as pd

# Load the raw dataset
raw_data = pd.read_csv('../data/raw/train.csv')

# Display basic info
print("=== Raw Data Overview ===")
print("Shape:", raw_data.shape)
print("Columns:", raw_data.columns.tolist())
print("\n=== Sample Data ===")
print(raw_data.head())

# Update critical columns (only those present in your dataset)
critical_columns = [
    'pickup_datetime', 'dropoff_datetime',
    'pickup_longitude', 'pickup_latitude',
    'dropoff_longitude', 'dropoff_latitude',
    'trip_duration'
]

# Drop rows with missing critical fields
cleaned_data = raw_data.dropna(subset=critical_columns)

# Remove duplicates
cleaned_data = cleaned_data.drop_duplicates()

# Convert timestamps to datetime
cleaned_data['pickup_datetime'] = pd.to_datetime(cleaned_data['pickup_datetime'])
cleaned_data['dropoff_datetime'] = pd.to_datetime(cleaned_data['dropoff_datetime'])

# Remove invalid timestamps
cleaned_data = cleaned_data[cleaned_data['dropoff_datetime'] >= cleaned_data['pickup_datetime']]

# Define NYC bounds and remove invalid coordinates
nyc_bounds = {
    'min_lat': 40.4774, 'max_lat': 40.9176,
    'min_lon': -74.2591, 'max_lon': -73.7004
}

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

# Round coordinates
coordinate_columns = [
    'pickup_longitude', 'pickup_latitude',
    'dropoff_longitude', 'dropoff_latitude'
]
for col in coordinate_columns:
    cleaned_data[col] = cleaned_data[col].round(6)

# Derived feature: trip_duration_min (already in seconds, convert to minutes)
cleaned_data['trip_duration_min'] = cleaned_data['trip_duration'] / 60

# Log excluded records
excluded_records = raw_data[~raw_data.index.isin(cleaned_data.index)]
excluded_records.to_csv('../data/processed/excluded_records.csv', index=False)

# Save cleaned data
cleaned_data.to_csv('../data/processed/cleaned_trips.csv', index=False)

# Display cleaned data info
print("\n=== Cleaned Data Overview ===")
print("Shape:", cleaned_data.shape)
print("Columns:", cleaned_data.columns.tolist())
print("\n=== Sample Cleaned Data ===")
print(cleaned_data.head())
