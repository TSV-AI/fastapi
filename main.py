import os
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
# If using Hypercorn as per the template, uvicorn import isn't strictly needed here
# import uvicorn 

app = FastAPI()

# This MUST match the mount path you set in Railway for this service's volume,
# AND it's where your "Crawler-2.2" service saves the CSV.
VOLUME_MOUNT_PATH = "/data" 
CSV_FILENAME = "complete_venues.csv"
csv_file_path = os.path.join(VOLUME_MOUNT_PATH, CSV_FILENAME)

@app.get("/download/csv")
async def download_csv_file():
    """
    Provides the CSV file for download if it exists.
    """
    if not os.path.exists(csv_file_path):
        raise HTTPException(
            status_code=404, 
            detail=f"'{CSV_FILENAME}' not found at '{csv_file_path}'. "
                   f"Ensure the scraper ('Crawler-2.2') has run successfully and saved the file, "
                   f"and that this downloader service has the volume correctly mounted at '{VOLUME_MOUNT_PATH}'."
        )

    return FileResponse(
        path=csv_file_path,
        filename=CSV_FILENAME, # This name will be suggested to the user when they download
        media_type='text/csv',
        headers={"Content-Disposition": f"attachment; filename=\"{CSV_FILENAME}\""} # Crucial for triggering download
    )

@app.get("/")
async def root():
    """
    Root endpoint to confirm the service is running.
    """
    return {
        "message": "CSV Downloader Service is running.",
        "download_link": "/download/csv",
        "serving_file_from_path": csv_file_path
    }

# Note: You don't need the 'if __name__ == "__main__": uvicorn.run(...)' block
# if Railway is using a start command like 'hypercorn main:app ...' or 'uvicorn main:app ...'
