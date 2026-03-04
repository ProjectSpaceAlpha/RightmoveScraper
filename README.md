# Rightmove Scraper & Market Analytics

A comprehensive toolset for reverse-engineering Rightmove search parameters, scraping property data, and visualizing market insights on an interactive map.

## 🏗️ Project Structure

The project is split into two main components:
1.  **Backend (Python/FastAPI)**: A scraping engine that handles the heavy lifting of extracting data from Rightmove using Playwright and BeautifulSoup.
2.  **Frontend (React/Vite)**: A modern web interface for building queries, managing scrapers, and analyzing property data via an interactive map and insights dashboard.

---

## 🚀 Getting Started

### 1. Prerequisites
- **Python 3.10+**
- **Node.js 18+**
- **Google Maps API Key** (Optional - Defaults to OpenStreetMap. Required for Satellite view and Station Walking Directions)

### 2. Backend Setup
Navigate to the root directory:
```bash
# Install dependencies
pip install fastapi uvicorn requests beautifulsoup4 playwright pandas pydantic

# Install Playwright browsers
playwright install chromium
```

Start the API server:
```bash
python api.py
```
The API will be available at `http://localhost:8000`.

### 3. Frontend Setup
Navigate to the web application directory:
```bash
cd RightmoveScraperWebApp

# Install dependencies
npm install

# Setup environment variables (Optional)
# Create a .env file in RightmoveScraperWebApp/ with:
VITE_GOOGLE_MAPS_API_KEY=your_api_key_here
VITE_GOOGLE_MAPS_MAP_ID=your_map_id_here
```

Start the development server:
```bash
npm run dev
```
The application will be available at `http://localhost:5173`.

---

## 🛠️ Features

### 🔍 Query Builder
- **Reverse Engineer URL**: Paste any Rightmove search URL to automatically populate the configuration form.
- **Batch Generation**: Generate multiple search URLs for different outcodes (e.g., AL8, SG1) simultaneously.
- **Parameter Control**: Fine-tune location, radius, price range, bedrooms, property types, and more.

### 🕷️ Scraping Pipeline
- **Real-time Progress**: Monitor the scraping status directly from the web UI.
- **JSON Data Handling**: Save and load your scraped datasets locally in JSON format.
- **Automated Processing**: Extract addresses, prices, square footage, and images automatically.

### 🗺️ Map & Analytics
- **Interactive Map View**: See all scraped properties on a map with custom markers.
- **Smart Fallback**: Automatically uses **OpenStreetMap** if no Google Maps API key is provided, ensuring the app is always functional.
- **Station Directions**: Toggle walking routes to the nearest train station (Requires Google Maps API).
- **Market Insights Dashboard**: Open the "📊 View Market Insights" overlay to see:
    - Price distributions (High/Low/Avg).
    - Bedroom & Property Type breakdowns (Pie charts).
    - Listing Age trends (Bucketed area chart).
    - Price vs. Square Footage value mapping (Scatter plot).

---

## 📸 Screenshots

*   **Bulk Search & Outcode Generation**: [View Screenshot](https://imgbox.com/FYQrykP3)
*   **Interactive Map Results**: [View Screenshot](https://imgbox.com/yU7YwYX6)
*   **Property Detail Overlays**: [View Screenshot](https://imgbox.com/fMiUYkhZ)
*   **Market Analytics Dashboard**: [View Screenshot](https://imgbox.com/uwyaIGPX)

---


## 📡 API Endpoints

- `POST /api/scrape/start`: Initiates a background scrape task for a list of URLs.
- `GET /api/scrape/status/{task_id}`: Returns the current progress of a task.
- `GET /api/scrape/results/{task_id}`: Retrieves the final dataset once completed.

---

## 🎨 Technologies Used

- **Frontend**: React 19, Vite, Recharts (Data Viz), Leaflet (Map Fallback), Lucide React (Icons), Google Maps API.
- **Backend**: FastAPI (Python), Playwright (Browser Automation), BeautifulSoup4 (HTML Parsing).
- **Styling**: Vanilla CSS with modern glassmorphism aesthetics.

---

## ⚠️ Disclaimer
This tool is for educational purposes. Ensure you comply with Rightmove's Terms of Service and robot.txt permissions when using scraping tools.
