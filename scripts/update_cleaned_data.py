# Attorney Valois NIYIGABA: I created this script to update the cleaned NYC taxi trip dataset with additional derived features.
# I added pickup_hour and pickup_day columns to enable time-based analysis and filtering.

import pandas as pd

# I loaded the already cleaned dataset from the processed directory.
cleaned_data = pd.read_csv('../data/processed/cleaned_trips.csv')

# I converted the pickup_datetime column to datetime format to extract time-based features.
cleaned_data['pickup_datetime'] = pd.to_datetime(cleaned_data['pickup_datetime'])

# I derived the pickup_hour feature from pickup_datetime to analyze trips by hour of the day.
cleaned_data['pickup_hour'] = cleaned_data['pickup_datetime'].dt.hour

# I derived the pickup_day feature from pickup_datetime to analyze trips by day of the week.
cleaned_data['pickup_day'] = cleaned_data['pickup_datetime'].dt.day_name()

# I saved the updated dataset back to the processed directory, overwriting the previous version.
cleaned_data.to_csv('../data/processed/cleaned_trips.csv', index=False)

# I printed an overview of the updated data to verify the new features.
print("\n=== Updated Cleaned Data Overview ===")
print("Shape:", cleaned_data.shape)
print("Columns:", cleaned_data.columns.tolist())
print("\n=== Sample Updated Cleaned Data ===")
print(cleaned_data.head())
