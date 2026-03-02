import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import os
import uuid
import json
import threading

from scraper import process_search_url

app = FastAPI(title="Rightmove Scraper API")

# Setup CORS for the React Frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5174", "http://localhost:5173", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Persistent store for task statuses and results
TASKS_DIR = "temp_tasks"
if not os.path.exists(TASKS_DIR):
    os.makedirs(TASKS_DIR)

tasks: Dict[str, Dict[str, Any]] = {}

def save_task(task_id: str):
    task_path = os.path.join(TASKS_DIR, f"{task_id}.json")
    with open(task_path, 'w') as f:
        json.dump(tasks[task_id], f)

def load_tasks():
    if not os.path.exists(TASKS_DIR):
        return
    for filename in os.listdir(TASKS_DIR):
        if filename.endswith(".json"):
            task_id = filename[:-5]
            try:
                with open(os.path.join(TASKS_DIR, filename), 'r') as f:
                    tasks[task_id] = json.load(f)
            except Exception as e:
                print(f"Error loading task {task_id}: {e}")

# Load existing tasks on startup
load_tasks()

class ScrapeRequest(BaseModel):
    urls: List[str]

import math

def clean_data(data):
    """Recursively replace NaN, Infinity, -Infinity with None for JSON compliance."""
    if isinstance(data, list):
        return [clean_data(item) for item in data]
    elif isinstance(data, dict):
        return {k: clean_data(v) for k, v in data.items()}
    elif isinstance(data, float):
        if math.isnan(data) or math.isinf(data):
            return None
    return data

def run_scrape_background(task_id: str, urls: List[str]):
    all_results = []
    tasks[task_id]["total"] = len(urls)
    
    for i, url in enumerate(urls):
        tasks[task_id]["current"] = i + 1
        tasks[task_id]["status"] = f"Processing {i+1}/{len(urls)}"
        save_task(task_id)
        
        output_filename = f"temp_scrape_{task_id}_{i}.csv"
        try:
            results = process_search_url(search_url=url, output_csv=output_filename, return_data=True)
            if results:
                cleaned_results = clean_data(results)
                all_results.extend(cleaned_results)
                
            # Clean up temp file
            if os.path.exists(output_filename):
                os.remove(output_filename)
                
        except Exception as e:
            print(f"Error processing {url}: {e}")
            
    tasks[task_id]["status"] = "completed"
    tasks[task_id]["results"] = all_results
    save_task(task_id)

@app.post("/api/scrape/start")
async def start_scrape(request: ScrapeRequest, background_tasks: BackgroundTasks):
    if not request.urls:
        raise HTTPException(status_code=400, detail="No URLs provided")
    
    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        "status": "starting",
        "current": 0,
        "total": len(request.urls),
        "results": None
    }
    save_task(task_id)
    
    background_tasks.add_task(run_scrape_background, task_id, request.urls)
    return {"task_id": task_id}

@app.get("/api/scrape/status/{task_id}")
async def get_scrape_status(task_id: str):
    # Try to reload from disk if not in memory
    if task_id not in tasks:
        task_path = os.path.join(TASKS_DIR, f"{task_id}.json")
        if os.path.exists(task_path):
            with open(task_path, 'r') as f:
                tasks[task_id] = json.load(f)
        else:
            raise HTTPException(status_code=404, detail="Task not found")
    
    return {
        "status": tasks[task_id]["status"],
        "current": tasks[task_id]["current"],
        "total": tasks[task_id]["total"]
    }

@app.get("/api/scrape/results/{task_id}")
async def get_scrape_results(task_id: str):
    if task_id not in tasks:
        task_path = os.path.join(TASKS_DIR, f"{task_id}.json")
        if os.path.exists(task_path):
            with open(task_path, 'r') as f:
                tasks[task_id] = json.load(f)
        else:
            raise HTTPException(status_code=404, detail="Task not found")
    
    if tasks[task_id]["status"] != "completed":
        raise HTTPException(status_code=400, detail="Task not yet completed")
    
    return tasks[task_id]["results"]

if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000)

