# scripts/update_cleaned_data.py
import pandas as pd

# Load the already cleaned data
cleaned_data = pd.read_csv('../data/processed/cleaned_trips.csv')

# Convert pickup_datetime to datetime (required for extracting hour and day)
cleaned_data['pickup_datetime'] = pd.to_datetime(cleaned_data['pickup_datetime'])

# Derived feature 2: pickup_hour
cleaned_data['pickup_hour'] = cleaned_data['pickup_datetime'].dt.hour

# Derived feature 3: pickup_day
cleaned_data['pickup_day'] = cleaned_data['pickup_datetime'].dt.day_name()

# Save the updated cleaned data
cleaned_data.to_csv('../data/processed/cleaned_trips.csv', index=False)

# Display updated cleaned data info
print("\n=== Updated Cleaned Data Overview ===")
print("Shape:", cleaned_data.shape)
print("Columns:", cleaned_data.columns.tolist())
print("\n=== Sample Updated Cleaned Data ===")
print(cleaned_data.head())
