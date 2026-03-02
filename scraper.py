import re
import json
import logging
import concurrent.futures
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

import pandas as pd
from curl_cffi import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://www.rightmove.co.uk"

def fetch_with_retry(url: str, max_retries: int = 3, backoff_factor: float = 1.5) -> Optional[requests.Response]:
    """Fetches a URL using curl_cffi to spoof TLS fingerprint, with exponential backoff on failure."""
    for attempt in range(max_retries):
        try:
            # Impersonating Chrome to bypass Datadome/Cloudflare WAF
            response = requests.get(url, impersonate="chrome120", timeout=15)
            if response.status_code == 200:
                return response
            elif response.status_code in [403, 502, 503]:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed with status {response.status_code} for URL: {url}")
            else:
                logger.error(f"Unexpected status code {response.status_code} for URL: {url}")
                return response
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries} encountered exception: {e} for URL: {url}")
        
        if attempt < max_retries - 1:
            sleep_time = backoff_factor ** attempt
            logger.info(f"Retrying in {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
            
    logger.error(f"Max retries ({max_retries}) reached for URL: {url}")
    return None

def extract_json_from_html(html: str, regex_pattern: str) -> Optional[str]:
    """Extracts a JSON string from HTML using a provided regex pattern."""
    match = re.search(regex_pattern, html, re.IGNORECASE | re.DOTALL)
    if match:
        return match.group(1)
    return None

def safe_json_decode(target_string: str) -> Optional[Dict[str, Any]]:
    """Safely decodes JSON by locating the matching closing brace, ignoring trailing Javascript artifacts."""
    target_string = target_string.strip()
    if not target_string.startswith('{'):
        return None
    
    brace_count = 0
    in_string = False
    escape_next = False
    valid_end_index = -1
    
    for i, char in enumerate(target_string):
        if escape_next:
            escape_next = False
            continue
        
        if char == '\\':
            escape_next = True
            continue
            
        if char == '"':
            in_string = not in_string
            continue
            
        if not in_string:
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    valid_end_index = i
                    break
                    
    if valid_end_index != -1:
        clean_json_string = target_string[:valid_end_index + 1]
        try:
            return json.loads(clean_json_string)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode parsed JSON: {e}")
            return None
            
    return None

def sweep_search_page(base_search_url: str, index: int) -> tuple[int, List[Dict[str, Any]]]:
    """Phase 1: Sweep a single pagination page of a Rightmove search."""
    # Ensure URL is clean and handle index
    if '?' in base_search_url:
        page_url = f"{base_search_url}&index={index}" if "index=" not in base_search_url else re.sub(r'index=\d+', f'index={index}', base_search_url)
    else:
        page_url = f"{base_search_url}?index={index}"

    logger.info(f"Sweeping search page: {page_url}")
    response = fetch_with_retry(page_url)
    if not response:
        return 0, []

    json_str = extract_json_from_html(response.text, r'<script[^>]*id="__NEXT_DATA__"[^>]*type="application/json"[^>]*>(.*?)</script>')
    if not json_str:
        logger.error(f"Could not find __NEXT_DATA__ JSON in {page_url}")
        return 0, []

    try:
        data = json.loads(json_str)
        search_results = data.get('props', {}).get('pageProps', {}).get('searchResults', {})
        
        # Parse total results safely
        raw_count = search_results.get('resultCount', '0')
        if isinstance(raw_count, str):
            total_results = int(raw_count.replace(',', ''))
        else:
            total_results = int(raw_count)
            
        properties = search_results.get('properties', [])
        
        clean_properties = []
        for prop in properties:
            prop_id = prop.get('id')
            url = prop.get('propertyUrl')
            if prop_id and url:
                if url.startswith('/'):
                    url = f"{BASE_URL}{url}"
                clean_properties.append({
                    'id': prop_id,
                    'url': url
                })
                
        return total_results, clean_properties
        
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from search page: {e}")
        return 0, []
    except Exception as e:
        logger.error(f"Unexpected error parsing search page data: {e}")
        return 0, []

def get_all_property_urls(base_search_url: str) -> List[Dict[str, Any]]:
    """Phase 1 Driver: Iterate through all paginated search results to collect all property URLs."""
    all_properties = []
    index = 0
    total_expected = -1
    
    # First request establishes total count
    total_results, initial_properties = sweep_search_page(base_search_url, index)
    if total_results == 0 or not initial_properties:
         logger.warning("No results found on initial page.")
         return []
         
    all_properties.extend(initial_properties)
    logger.info(f"Found {total_results} total expected properties.")
    
    # Rightmove maxes out at index 1008 (42 pages of 24)
    # But we calculate based on total_results to be safe
    max_index = min(((total_results - 1) // 24) * 24, 1008) 
    
    if max_index > 0:
        indices_to_fetch = list(range(24, max_index + 24, 24))
        logger.info(f"Fetching {len(indices_to_fetch)} additional paginated search pages concurrently...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_index = {executor.submit(sweep_search_page, base_search_url, idx): idx for idx in indices_to_fetch}
            for future in concurrent.futures.as_completed(future_to_index):
                idx = future_to_index[future]
                try:
                    _, properties = future.result()
                    all_properties.extend(properties)
                    logger.info(f"Pagination index {idx} fetched {len(properties)} properties.")
                except Exception as exc:
                    logger.error(f"Pagination index {idx} generated an exception: {exc}")
                    
    # Deduplicate properties by ID just in case
    unique_properties = {prop['id']: prop for prop in all_properties}.values()
    logger.info(f"Successfully swept {len(unique_properties)} unique property URLs.")
    return list(unique_properties)

def parse_date(date_str: str, date_format: str) -> Optional[datetime]:
    """Helper to parse dates safely."""
    try:
        return datetime.strptime(date_str, date_format)
    except (ValueError, TypeError):
        return None

def deep_dive_property(prop_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Phase 2: Deep-dive into an individual property page to extract full details."""
    url = prop_dict.get('url')
    prop_id = prop_dict.get('id')
    logger.debug(f"Deep-diving: {url}")
    
    response = fetch_with_retry(url)
    if not response:
        return None
        
    # Extract window.PAGE_MODEL json string
    json_str_raw = extract_json_from_html(response.text, r'window\.PAGE_MODEL\s*=\s*({.*?});?</script>')
    if not json_str_raw:
        logger.warning(f"Could not extract PAGE_MODEL JSON for {url}")
        return None
        
    # Safely decode ignoring trailing JS garbage
    data = safe_json_decode(json_str_raw)
    if not data:
        logger.warning(f"Could not safely JSON decode PAGE_MODEL for {url}")
        return None
        
    try:
        property_data = data.get('propertyData', {})
        analytics_info = data.get('analyticsInfo', {}).get('analyticsProperty', {})
        
        # 1. Base Info
        raw_price = property_data.get('prices', {}).get('primaryPrice', '')
        # Strip non-numeric
        clean_price_str = re.sub(r'[^\d]', '', raw_price)
        price = int(clean_price_str) if clean_price_str else None
        
        bedrooms = property_data.get('bedrooms')
        prop_type = property_data.get('propertySubType')
        address = property_data.get('address', {}).get('displayAddress')
        
        # 2. Spatial Data
        lat = property_data.get('location', {}).get('latitude')
        lon = property_data.get('location', {}).get('longitude')
        
        # 3. Arbitrage / Timing Signals
        # List Date formatted as YYYYMMDD
        raw_added_date = analytics_info.get('added')
        list_date = parse_date(raw_added_date, '%Y%m%d') if raw_added_date else None
        
        # Reduction Status
        update_reason = property_data.get('listingHistory', {}).get('listingUpdateReason', '')
        reduction_date = None
        if update_reason and update_reason.startswith('Reduced on'):
            # Extract date (e.g. DD/MM/YYYY or DD/MM/YY)
            date_match = re.search(r'(\d{2}/\d{2}/\d{4})', update_reason)
            if date_match:
                reduction_date = parse_date(date_match.group(1), '%d/%m/%Y')
        
        # 4. Property Features (New)
        nearest_stations = property_data.get('nearestStations', [])
        nearest_station_name = None
        nearest_station_distance = None
        if nearest_stations:
            station = nearest_stations[0]
            nearest_station_name = station.get('name')
            nearest_station_distance = station.get('distance')

        # Sqft extraction (sizings or floorplanAreas)
        sqft = None
        sizings = property_data.get('sizings', [])
        if sizings:
            # Look for sqft in maximumSize or minimumSize
            for size in sizings:
                if size.get('unit') == 'sqft':
                    sqft = size.get('maximumSize') or size.get('minimumSize')
                    break
        
        if not sqft:
            # Fallback to floorplanAreas
            floorplan_areas = property_data.get('floorplanAreas', [])
            for area in floorplan_areas:
                if area.get('unit') == 'sqft':
                    sqft = area.get('value')
                    break

        # Image URL (first photo)
        images = property_data.get('images', [])
        image_url = images[0].get('url') if images else None

        # 5. Phase 4: Time-Series Math Logic
        current_date_dt = datetime.now()
        
        days_on_market = None
        if list_date:
             days_on_market = (current_date_dt - list_date).days
             
        days_since_reduction = None
        if reduction_date:
             days_since_reduction = (current_date_dt - reduction_date).days
             
        days_to_reduce = None
        if list_date and reduction_date:
            days_to_reduce = (reduction_date - list_date).days

        return {
            'id': prop_id,
            'url': url,
            'price': price,
            'bedrooms': bedrooms,
            'type': prop_type,
            'address': address,
            'latitude': lat,
            'longitude': lon,
            'list_date': list_date.strftime('%Y-%m-%d') if list_date else None,
            'reduction_date': reduction_date.strftime('%Y-%m-%d') if reduction_date else None,
            'update_reason': update_reason,
            'days_on_market': days_on_market,
            'days_since_reduction': days_since_reduction,
            'days_to_reduce': days_to_reduce,
            'nearest_station_name': nearest_station_name,
            'nearest_station_distance': nearest_station_distance,
            'sqft': sqft,
            'image_url': image_url,
            'scraped_at': current_date_dt.strftime('%Y-%m-%d %H:%M:%S')
        }
    except Exception as e:
        logger.error(f"Error parsing detail JSON for {url}: {e}")
        return None

def process_search_url(search_url: str, output_csv: str = 'rightmove_arb_data.csv', return_data: bool = False) -> Optional[List[Dict[str, Any]]]:
    """Main pipeline execution entry point."""
    logger.info("="*50)
    logger.info(f"Starting Scraper Pipeline for: {search_url}")
    logger.info("="*50)
    
    # Phase 1: Pagination Sweep
    logger.info("▶ PHASE 1: Executing Search Pagination Sweep...")
    properties_to_scrape = get_all_property_urls(search_url)
    
    if not properties_to_scrape:
        logger.error("No properties found to scrape. Exiting pipeline.")
        return
        
    # Phase 2: Detail Deep Dive (Concurrent)
    logger.info(f"▶ PHASE 2: Executing Detail Deep-Dive for {len(properties_to_scrape)} properties...")
    results = []
    
    # Using ThreadPool with cautious max_workers to avoid instant WAF ban
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_prop = {executor.submit(deep_dive_property, prop): prop for prop in properties_to_scrape}
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_prop), 1):
            prop = future_to_prop[future]
            try:
                data = future.result()
                if data:
                    results.append(data)
                
                if i % 25 == 0:
                    logger.info(f"Progress: Processed {i}/{len(properties_to_scrape)} properties...")
            except Exception as exc:
                logger.error(f"Property {prop['url']} generated an exception: {exc}")

    # Phase 3: Export
    logger.info(f"▶ PHASE 3: Consolidating and Exporting {len(results)} records...")
    if results:
        df = pd.DataFrame(results)
        
        # Sort by best arbitrage candidates (e.g. highest days_on_market or recently reduced)
        if 'days_on_market' in df.columns:
             df = df.sort_values(by='days_on_market', ascending=False, na_position='last')
             
        df.to_csv(output_csv, index=False)
        logger.info(f"Pipeline Complete! Data successfully exported to {output_csv}")
        
        if return_data:
             # Convert NaN back to None for JSON serialization
             df = df.where(pd.notnull(df), None)
             return df.to_dict(orient='records')
             
    else:
        logger.error("No data successfully extracted to export.")
        if return_data:
             return []

if __name__ == "__main__":
    import sys
    
    # Simple CLI argument handling for quick testing
    target_url = "https://www.rightmove.co.uk/property-for-sale/find.html?searchLocation=London&useLocationIdentifier=true&locationIdentifier=OUTCODE%5E1108&radius=0.25&minPrice=170000&maxPrice=1500000"
    
    if len(sys.argv) > 1:
        target_url = sys.argv[1]
        
    process_search_url(target_url)
