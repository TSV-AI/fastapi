# main.py for your FastAPI service
import os
import asyncpg
import asyncio
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse # For sending CSV
import io # For creating CSV in memory
import csv # For creating CSV in memory

app = FastAPI()

DB_POOL = None # Global variable for the database connection pool

# FastAPI event handler for application startup
@app.on_event("startup")
async def startup_db_client():
    global DB_POOL
    try:
        # Railway typically provides DATABASE_URL pointing to the private connection
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            # Fallback if DATABASE_URL isn't set, construct from individual vars
            print("DEBUG: FastAPI - DATABASE_URL not found, constructing from individual PG* vars.")
            pg_user = os.getenv('PGUSER', os.getenv('POSTGRES_USER', 'postgres'))
            pg_password = os.getenv('PGPASSWORD', os.getenv('POSTGRES_PASSWORD'))
            pg_db = os.getenv('PGDATABASE', os.getenv('POSTGRES_DB', 'railway'))
            pg_host = os.getenv('PGHOST', 'postgres.railway.internal') # Default to private host
            pg_port = os.getenv('PGPORT', '5432')
            
            if not all([pg_user, pg_password, pg_db, pg_host]):
                raise ValueError("FastAPI - Missing one or more core database connection environment variables.")
            database_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"

        log_url = database_url
        temp_password_for_log = os.getenv('PGPASSWORD', os.getenv('POSTGRES_PASSWORD'))
        if temp_password_for_log: log_url = log_url.replace(temp_password_for_log, "********")
        print(f"DEBUG: FastAPI - Connecting to database using DSN: {log_url}")
        
        DB_POOL = await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=5)
        print("DEBUG: FastAPI - Database connection pool created successfully.")
    except Exception as e:
        DB_POOL = None
        print(f"DEBUG: FastAPI - CRITICAL ERROR - Could not connect to PostgreSQL on startup: {e}")
        # You might want the app to not fully start or indicate an error state if DB connection fails
        # For now, it will proceed, but endpoints requiring DB_POOL will fail.

# FastAPI event handler for application shutdown
@app.on_event("shutdown")
async def shutdown_db_client():
    global DB_POOL
    if DB_POOL:
        await DB_POOL.close()
        print("DEBUG: FastAPI - Database connection pool closed.")

@app.get("/download/csv")
async def download_csv_file():
    if not DB_POOL:
        raise HTTPException(status_code=503, detail="Database service not available. Check FastAPI service logs.")

    async with DB_POOL.acquire() as connection:
        try:
            # Fetch data from the 'venues' table. Ensure column names match your DB table.
            # These should match your Venue Pydantic model fields.
            rows = await connection.fetch("SELECT name, location, price, capacity, rating, reviews, description FROM venues")
            if not rows:
                raise HTTPException(status_code=404, detail="No venue data found in the database.")

            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write header (must match the order of selected columns)
            header = ["name", "location", "price", "capacity", "rating", "reviews", "description"]
            writer.writerow(header)
            
            for row in rows:
                # asyncpg rows can be accessed like dictionaries or by index
                writer.writerow([row[field] for field in header]) 

            output.seek(0) # Rewind buffer to the beginning
            
            return StreamingResponse(
                iter([output.getvalue()]),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename=\"complete_venues.csv\""}
            )
        except Exception as e:
            print(f"DEBUG: FastAPI - Error fetching or generating CSV: {e}")
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Error generating CSV: {str(e)}")

@app.get("/venues") # Optional: An endpoint to get data as JSON
async def get_venues_as_json():
    if not DB_POOL:
        raise HTTPException(status_code=503, detail="Database service not available.")
    async with DB_POOL.acquire() as connection:
        rows = await connection.fetch("SELECT name, location, price, capacity, rating, reviews, description FROM venues")
        return [dict(row) for row in rows]

@app.get("/")
async def root():
    return {"message": "FastAPI CSV Downloader Service from PostgreSQL is running. Try /download/csv or /venues."}
