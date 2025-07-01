import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def load_and_clean_data(file_path: str, skiprows: int = 2, start_row: int = 11) -> pd.DataFrame:
    """
    Load and clean the Excel data.

    Parameters:
    file_path (str): The path to the Excel file.
    skiprows (int): Number of rows to skip at the start (default is 2).
    start_row (int): The starting row to consider valid data (default is 11).

    Returns:
    pd.DataFrame: The cleaned DataFrame.
    """
    try:
        # Load the Excel file
        df = pd.read_excel(file_path, skiprows=skiprows, header=0)

        # Drop the 'Unnamed: 0' column if it exists
        df.drop(columns=['Unnamed: 0'], inplace=True, errors='ignore')

        # Drop rows before the start_row index
        df = df.iloc[start_row:].reset_index(drop=True)

        # Convert the 'ID' column to numeric and filter out non-numeric values
        df = df[pd.to_numeric(df['ID'], errors='coerce').notna()]

        # Reset the index after filtering
        df.reset_index(drop=True, inplace=True)

        return df

    except FileNotFoundError:
        print(f"File {file_path} not found.")
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

def create_database(df: pd.DataFrame, db_url: str, table_name: str = 'cars_data'):
    """
    Create a PostgreSQL database and store the DataFrame as a table.

    Parameters:
    df (pd.DataFrame): The DataFrame to be stored.
    db_url (str): The connection URL for the PostgreSQL database.
    table_name (str): The table name for storing the DataFrame (default is 'cars_data').
    """
    try:
        # Create a SQLAlchemy engine and store the data in the database using a context manager
        with create_engine(db_url).connect() as connection:
            df.to_sql(table_name, connection, if_exists='replace', index=False)
    
    except Exception as e:
        print(f"Database error: {e}")
        raise

# Load and clean the data
file_path = "cars_data.xlsx"  # Replace this with your file path
db_url = os.getenv("POSTGRES_URL") # Adjust for your setup

try:
    df = load_and_clean_data(file_path)

    # Create the PostgreSQL database and save the DataFrame
    create_database(df, db_url)

    # Print the DataFrame head and shape
    print(df.head())
    print(df.shape)

except Exception as e:
    print(f"Process failed: {e}")
