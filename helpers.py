# Fix function
def fix_datetime_format(date_str):
    parts = date_str.split("-")
    year, month, day, hour = map(int, parts)  # Convert all parts to integers
    
    if hour == 24:
        hour = 0  # Reset hour to 00
        new_date = pd.Timestamp(year, month, day) + pd.Timedelta(days=1)  # Add a day
        return new_date.replace(hour=hour)  # Set hour to 00
    else:
        return pd.Timestamp(year, month, day, hour)

# Apply the fix
data["correct_days"] = data["correct_days"].apply(fix_datetime_format)
# Create a unique numerical index
#data['unique_id'] = range(1, len(data)+1)  # Ensuring uniqueness