from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
import psycopg2
import os
from typing import List, Dict
from vanna.openai import OpenAI_Chat
from vanna.chromadb import ChromaDB_VectorStore
import uvicorn
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Define the response model using Pydantic
class CarResponse(BaseModel):
    data: List[Dict]
    sql_query: str

class MyVanna(ChromaDB_VectorStore, OpenAI_Chat):
    def __init__(self, config=None):
        ChromaDB_VectorStore.__init__(self, config=config)
        OpenAI_Chat.__init__(self, config=config)

# Initialize the Vanna class with environment variables
vn = MyVanna(config={
    'api_key': os.getenv('OPENAI_API_KEY'),
    'model': 'gpt-4-turbo'
})

vn.connect_to_postgres(
    host=os.getenv('POSTGRES_HOST'),  # Docker host
    dbname=os.getenv('POSTGRES_DB'),  # Set by POSTGRES_DB
    user=os.getenv('POSTGRES_USER'),  # Set by POSTGRES_USER
    password=os.getenv('POSTGRES_PASSWORD'),  # Set by POSTGRES_PASSWORD
    port=os.getenv('POSTGRES_PORT')  # The port exposed by Docker
)

# Connect to PostgreSQL
def get_db_connection():
    """Create and return a new PostgreSQL database connection."""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),  # Docker host
        dbname=os.getenv('POSTGRES_DB'),  # Set by POSTGRES_DB
        user=os.getenv('POSTGRES_USER'),  # Set by POSTGRES_USER
        password=os.getenv('POSTGRES_PASSWORD'),  # Set by POSTGRES_PASSWORD
        port=os.getenv('POSTGRES_PORT')  # The port exposed by Docker
    )

def generate_sql_query(user_query: str) -> str:
    """Generate an SQL query from user input using Vanna."""
    return vn.generate_sql(question=user_query, allow_llm_to_see_data=True)

# Create a FastAPI application
app = FastAPI()

@app.get("/cars", response_model=CarResponse)
async def read_cars(query: str = Query(..., description="Enter your prompt to retrieve car data.")):
    """Retrieve car data based on the generated SQL query."""

    try:
        sql_query = generate_sql_query(user_query=query)
        print("sql_query", sql_query)

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                
                # Commit changes for non-SELECT queries (UPDATE, DELETE, INSERT, ALTER, etc.)
                query_type = sql_query.strip().split()[0].lower()  # Get the type of query (e.g., 'update', 'delete', etc.)
                
                if query_type in ["update", "delete", "insert", "alter", "create", "drop"]:
                    conn.commit()  # Commit the changes for modification queries
                    return {"message": f"{query_type.capitalize()} operation successful", "sql_query": sql_query}
                else:
                    # Fetch results only for SELECT queries
                    cars = cursor.fetchall()
                    column_names = [desc[0] for desc in cursor.description]
                    return CarResponse(data=[dict(zip(column_names, car)) for car in cars], sql_query=sql_query)

    except psycopg2.Error:
        raise HTTPException(status_code=500, detail=f"Invalid SQL query syntax. Please revise your query.")
    except Exception as e:
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")

            
        
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)







