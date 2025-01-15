import pandas as pd
import pymysql

# Database configuration
db_config = {
    'host': 'localhost',       # Change to your MySQL server address
    'user': 'root',   # Replace with your MySQL username
    'password': 'rohit',  # Replace with your MySQL password
    'database': 'sys'   # Replace with your target database name
}

# File and table details
csv_file = 'final_output_reordered-2.csv'
table_name = 'CompanyData'

# Connect to the database
try:
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    # Load the CSV file into a DataFrame
    data = pd.read_csv(csv_file)
    print(f"Loaded {len(data)} rows from {csv_file}")

    # Replace NaN values in the DataFrame with None (which is equivalent to NULL in SQL)
    data = data.where(pd.notnull(data), None)

    # Dynamically create the INSERT statement
    columns = ', '.join(data.columns)
    placeholders = ', '.join(['%s'] * len(data.columns))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    # Iterate over the rows in the DataFrame and execute the INSERT query
    for row in data.itertuples(index=False):
        cursor.execute(insert_query, tuple(row))

    # Commit the transaction
    connection.commit()
    print(f"Data successfully inserted into the {table_name} table.")

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close the connection
    if connection:
        connection.close()
        print("MySQL connection closed.")
