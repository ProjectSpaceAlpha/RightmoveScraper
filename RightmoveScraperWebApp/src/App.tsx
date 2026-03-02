import { useState, useCallback, useEffect } from 'react';
import './App.css';
import MapView from './MapView';
import type { PropertyData } from './MapView';
import MapSidebar from './MapSidebar';
import type { MapFilters } from './MapSidebar';
import StatsOverlay from './StatsOverlay';
import HelpOverlay from './HelpOverlay';

// Standard Rightmove Parameters for Property For Sale
const PROPERTY_TYPES = [
  { value: 'detached', label: 'Detached' },
  { value: 'semi-detached', label: 'Semi-Detached' },
  { value: 'terraced', label: 'Terraced' },
  { value: 'flats', label: 'Flats / Apartments' },
  { value: 'bungalow', label: 'Bungalows' },
  { value: 'land', label: 'Land' },
  { value: 'commercial', label: 'Commercial' },
];

const TENURE_TYPES = [
  { value: 'FREEHOLD', label: 'Freehold' },
  { value: 'LEASEHOLD', label: 'Leasehold' },
  { value: 'SHARE_OF_FREEHOLD', label: 'Share of Freehold' },
];

const MUST_HAVES = [
  { value: 'garden', label: 'Garden' },
  { value: 'parking', label: 'Parking' },
  { value: 'retirement', label: 'Retirement Home' },
  { value: 'sharedOwnership', label: 'Shared Ownership' },
  { value: 'student', label: 'Student Accommodation' }
];

const MAX_DAYS = [
  { value: '', label: 'Anytime' },
  { value: '1', label: 'Last 24 hours' },
  { value: '3', label: 'Last 3 days' },
  { value: '7', label: 'Last 7 days' },
  { value: '14', label: 'Last 14 days' }
];

interface QueryParams {
  searchLocation: string;
  locationIdentifier: string;
  displayLocationIdentifier: string;
  radius: string;
  minPrice: string;
  maxPrice: string;
  minBedrooms: string;
  maxBedrooms: string;
  minBathrooms: string;
  maxBathrooms: string;
  propertyTypes: string[];
  tenureTypes: string[];
  mustHave: string[];
  maxDaysSinceAdded: string;
  _includeSSTC: boolean;
  index: string;
  sortType: string;
  channel: string;
  transactionType: string;
}

const DEFAULT_PARAMS: QueryParams = {
  searchLocation: '',
  locationIdentifier: '',
  displayLocationIdentifier: '',
  radius: '0.0',
  minPrice: '',
  maxPrice: '',
  minBedrooms: '',
  maxBedrooms: '',
  minBathrooms: '',
  maxBathrooms: '',
  propertyTypes: [],
  tenureTypes: [],
  mustHave: [],
  maxDaysSinceAdded: '',
  _includeSSTC: false,
  index: '0',
  sortType: '2', // Highest price first usually, or 6 for newest
  channel: 'BUY',
  transactionType: 'BUY'
};

function App() {
  const [urlInput, setUrlInput] = useState('');
  const [params, setParams] = useState<QueryParams>(DEFAULT_PARAMS);
  const [generatedUrl, setGeneratedUrl] = useState('');
  const [copied, setCopied] = useState(false);
  const [isConfigOpen, setIsConfigOpen] = useState(false);

  // Batch outcode state
  const [batchOutcodes, setBatchOutcodes] = useState('');
  const [batchUrls, setBatchUrls] = useState<string[]>([]);
  const [isGeneratingBatch, setIsGeneratingBatch] = useState(false);
  const [batchCopied, setBatchCopied] = useState(false);

  // Scraper State
  const [isScraping, setIsScraping] = useState(false);
  const [scrapedData, setScrapedData] = useState<PropertyData[]>([]);
  const [autoDownload, setAutoDownload] = useState(false);

  // View State
  const [currentView, setCurrentView] = useState<'builder' | 'map'>('builder');
  const [showStats, setShowStats] = useState(false);
  const [showHelp, setShowHelp] = useState(false);

  // Map Filters
  const [mapFilters, setMapFilters] = useState<MapFilters>({
    maxDaysOnMarket: null,
    reducedOnly: false,
    maxStationDistance: null,
    minSqft: null,
    notReducedOnly: false,
    minPrice: null,
    maxPrice: null,
    propertyTypes: [],
    showStationRoutes: true
  });

  // Derive filtered properties for the map
  const filteredProperties = scrapedData.filter(p => {
    if (mapFilters.maxDaysOnMarket !== null) {
      const days = Number(p.days_on_market);
      if (isNaN(days) || days > mapFilters.maxDaysOnMarket) return false;
    }
    if (mapFilters.maxStationDistance !== null) {
      const distance = Number(p.nearest_station_distance);
      if (isNaN(distance) || distance > mapFilters.maxStationDistance) return false;
    }
    if (mapFilters.minSqft !== null) {
      const sqft = Number(p.sqft);
      if (isNaN(sqft) || sqft < mapFilters.minSqft) return false;
    }
    if (mapFilters.minPrice !== null) {
      const price = Number(p.price);
      if (isNaN(price) || price < mapFilters.minPrice) return false;
    }
    if (mapFilters.maxPrice !== null) {
      const price = Number(p.price);
      if (isNaN(price) || price > mapFilters.maxPrice) return false;
    }
    if (mapFilters.propertyTypes.length > 0) {
      const type = String(p.type || '');
      if (!mapFilters.propertyTypes.includes(type)) return false;
    }
    if (mapFilters.reducedOnly) {
      const reason = String(p.update_reason || '').toLowerCase();
      if (!reason.includes('reduced')) return false;
    }
    if (mapFilters.notReducedOnly) {
      const reason = String(p.update_reason || '').toLowerCase();
      if (reason.includes('reduced')) return false;
    }
    return true;
  });

  // JSON download helper
  const downloadJSON = (data: any[], filename: string) => {
    if (!data || data.length === 0) return;
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.setAttribute('href', url);
    link.setAttribute('download', filename);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const [scrapeProgress, setScrapeProgress] = useState<{ current: number; total: number; status: string } | null>(null);

  const handleRunScraper = async (urls: string[]) => {
    setIsScraping(true);
    setScrapeProgress({ current: 0, total: urls.length, status: 'Starting...' });

    try {
      // 1. Start the background task
      const startRes = await fetch('http://localhost:8000/api/scrape/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ urls }),
      });

      if (!startRes.ok) throw new Error('Failed to start scraping task');
      const { task_id } = await startRes.json();

      // 2. Poll for status
      const poll = async (): Promise<PropertyData[]> => {
        return new Promise((resolve, reject) => {
          const interval = setInterval(async () => {
            try {
              const statusRes = await fetch(`http://localhost:8000/api/scrape/status/${task_id}`);
              if (!statusRes.ok) throw new Error('Failed to check status');

              const statusData = await statusRes.json();
              setScrapeProgress({
                current: statusData.current,
                total: statusData.total,
                status: statusData.status
              });

              if (statusData.status === 'completed') {
                clearInterval(interval);
                // 3. Get results
                setScrapeProgress({ ...statusData, status: 'Downloading results...' });
                try {
                  const resultsRes = await fetch(`http://localhost:8000/api/scrape/results/${task_id}`);
                  if (!resultsRes.ok) throw new Error('Failed to fetch final results from server');
                  resolve(await resultsRes.json());
                } catch (fetchErr) {
                  reject(new Error('Network error downloading results. The data is saved on the server - please refresh and try again or check connection.'));
                }
              }
            } catch (err) {
              clearInterval(interval);
              reject(err);
            }
          }, 2000); // Poll every 2 seconds
        });
      };

      const data = await poll();

      if (data && data.length > 0) {
        setScrapedData(data);
        if (autoDownload) {
          downloadJSON(data, 'rightmove_scraped_data.json');
        }
        setCurrentView('map');
      } else {
        alert('Scraper returned no results.');
      }
    } catch (error) {
      console.error("Scraper Error:", error);
      const msg = error instanceof Error ? error.message : String(error);
      if (msg.includes('Failed to fetch')) {
        alert('Connection to backend lost. Since the scrape reached 100%, your data is likely safe on the server. Please check if the backend is running and refresh to try retrieving the results again.');
      } else {
        alert('Error: ' + msg);
      }
    } finally {
      setIsScraping(false);
      setScrapeProgress(null);
    }
  };

  // Parse URL to State
  const parseUrl = useCallback((url: string) => {
    try {
      const urlObj = new URL(url);
      if (!urlObj.hostname.includes('rightmove.co.uk')) {
        alert('Please paste a valid Rightmove URL');
        return;
      }

      const searchParams = urlObj.searchParams;
      const newParams = { ...DEFAULT_PARAMS };

      // Helper to extract comma separated values
      const getArray = (key: string) => {
        const val = searchParams.get(key);
        return val ? val.split(',') : [];
      };

      newParams.searchLocation = searchParams.get('searchLocation') || '';
      newParams.locationIdentifier = searchParams.get('locationIdentifier') || '';
      newParams.displayLocationIdentifier = searchParams.get('displayLocationIdentifier') || '';
      newParams.radius = searchParams.get('radius') || '0.0';
      newParams.minPrice = searchParams.get('minPrice') || '';
      newParams.maxPrice = searchParams.get('maxPrice') || '';
      newParams.minBedrooms = searchParams.get('minBedrooms') || '';
      newParams.maxBedrooms = searchParams.get('maxBedrooms') || '';
      newParams.minBathrooms = searchParams.get('minBathrooms') || '';
      newParams.maxBathrooms = searchParams.get('maxBathrooms') || '';
      newParams.maxDaysSinceAdded = searchParams.get('maxDaysSinceAdded') || '';
      newParams._includeSSTC = searchParams.get('_includeSSTC') === 'on' || searchParams.get('includeSSTC') === 'true';
      newParams.sortType = searchParams.get('sortType') || '2';

      newParams.propertyTypes = getArray('propertyTypes');
      newParams.tenureTypes = getArray('tenureTypes');
      newParams.mustHave = getArray('mustHave');

      setParams(newParams);
      setUrlInput(url);
    } catch (e) {
      console.warn("Invalid URL pasted");
    }
  }, []);

  // Sync state to URL
  useEffect(() => {
    const baseUrl = 'https://www.rightmove.co.uk/property-for-sale/find.html';
    const searchParams = new URLSearchParams();

    // Standard params
    if (params.searchLocation) searchParams.set('searchLocation', params.searchLocation);
    if (params.locationIdentifier) {
      searchParams.set('useLocationIdentifier', 'true');
      searchParams.set('locationIdentifier', params.locationIdentifier);
    }
    if (params.displayLocationIdentifier) searchParams.set('displayLocationIdentifier', params.displayLocationIdentifier);
    if (params.radius && params.radius !== '0.0') searchParams.set('radius', params.radius);

    if (params.minPrice) searchParams.set('minPrice', params.minPrice);
    if (params.maxPrice) searchParams.set('maxPrice', params.maxPrice);
    if (params.minBedrooms) searchParams.set('minBedrooms', params.minBedrooms);
    if (params.maxBedrooms) searchParams.set('maxBedrooms', params.maxBedrooms);
    if (params.minBathrooms) searchParams.set('minBathrooms', params.minBathrooms);
    if (params.maxBathrooms) searchParams.set('maxBathrooms', params.maxBathrooms);
    if (params.maxDaysSinceAdded) searchParams.set('maxDaysSinceAdded', params.maxDaysSinceAdded);

    if (params._includeSSTC) searchParams.set('_includeSSTC', 'on');

    // Arrays
    if (params.propertyTypes.length > 0) searchParams.set('propertyTypes', params.propertyTypes.join(','));
    if (params.tenureTypes.length > 0) searchParams.set('tenureTypes', params.tenureTypes.join(','));
    if (params.mustHave.length > 0) searchParams.set('mustHave', params.mustHave.join(','));

    // Technical defaults
    searchParams.set('index', params.index);
    searchParams.set('sortType', params.sortType);
    searchParams.set('channel', params.channel);
    searchParams.set('transactionType', params.transactionType);

    setGeneratedUrl(`${baseUrl}?${searchParams.toString()}`);
  }, [params]);

  const handleCopy = () => {
    navigator.clipboard.writeText(generatedUrl);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const updateParam = (key: keyof QueryParams, value: any) => {
    setParams(prev => ({ ...prev, [key]: value }));
  };

  const toggleArrayItem = (key: 'propertyTypes' | 'tenureTypes' | 'mustHave', value: string) => {
    setParams(prev => {
      const current = prev[key];
      const updated = current.includes(value)
        ? current.filter(item => item !== value)
        : [...current, value];
      return { ...prev, [key]: updated };
    });
  };

  const generateBatchUrls = async () => {
    if (!batchOutcodes.trim()) return;
    setIsGeneratingBatch(true);
    setBatchUrls([]);

    const outcodes = batchOutcodes.split(/[\s,]+/).filter(Boolean);
    const newUrls: string[] = [];
    const baseUrl = 'https://www.rightmove.co.uk/property-for-sale/find.html';

    for (const outcode of outcodes) {
      try {
        const cleanOutcode = outcode.trim().toUpperCase();
        const response = await fetch(`/api/rightmove/typeahead?query=${cleanOutcode}&limit=1&exclude=STREET`);
        const json = await response.json();

        let locId = '';
        if (response.ok && json.matches && json.matches.length > 0) {
          const match = json.matches[0];
          if (match.type === 'OUTCODE') {
            locId = `OUTCODE^${match.id}`;
          }
        }

        if (locId) {
          const searchParams = new URLSearchParams();
          searchParams.set('searchLocation', cleanOutcode);
          searchParams.set('useLocationIdentifier', 'true');
          searchParams.set('locationIdentifier', locId);

          // Map the rest of the parameters to the new search
          if (params.radius && params.radius !== '0.0') searchParams.set('radius', params.radius);
          if (params.minPrice) searchParams.set('minPrice', params.minPrice);
          if (params.maxPrice) searchParams.set('maxPrice', params.maxPrice);
          if (params.minBedrooms) searchParams.set('minBedrooms', params.minBedrooms);
          if (params.maxBedrooms) searchParams.set('maxBedrooms', params.maxBedrooms);
          if (params.minBathrooms) searchParams.set('minBathrooms', params.minBathrooms);
          if (params.maxBathrooms) searchParams.set('maxBathrooms', params.maxBathrooms);
          if (params.maxDaysSinceAdded) searchParams.set('maxDaysSinceAdded', params.maxDaysSinceAdded);
          if (params._includeSSTC) searchParams.set('_includeSSTC', 'on');
          if (params.propertyTypes.length > 0) searchParams.set('propertyTypes', params.propertyTypes.join(','));
          if (params.tenureTypes.length > 0) searchParams.set('tenureTypes', params.tenureTypes.join(','));
          if (params.mustHave.length > 0) searchParams.set('mustHave', params.mustHave.join(','));

          searchParams.set('index', params.index);
          searchParams.set('sortType', params.sortType);
          searchParams.set('channel', params.channel);
          searchParams.set('transactionType', params.transactionType);

          newUrls.push(`${baseUrl}?${searchParams.toString()}`);
        } else {
          console.warn(`Could not resolve outcode: ${outcode}`);
        }
      } catch (e) {
        console.error(`Error resolving outcode ${outcode}`, e);
      }
    }
    setBatchUrls(newUrls);
    setIsGeneratingBatch(false);
  };

  const handleFileUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e) => {
      const content = e.target?.result as string;
      try {
        const data = JSON.parse(content);
        const results = Array.isArray(data) ? data : (data.results || []);
        if (results.length > 0) {
          setScrapedData(results);
          setCurrentView('map');
        } else {
          alert('No property data found in JSON');
        }
      } catch (err) {
        alert('Failed to parse JSON file. Please ensure it is a valid JSON export.');
      }
    };
    reader.readAsText(file);
  };

  const handleCopyBatch = () => {
    navigator.clipboard.writeText(batchUrls.join('\n'));
    setBatchCopied(true);
    setTimeout(() => setBatchCopied(false), 2000);
  };

  // ---------- MAP VIEW ----------
  if (currentView === 'map' && scrapedData.length > 0) {
    const sqftCount = scrapedData.filter(p => p.sqft && !isNaN(Number(p.sqft))).length;

    return (
      <>
        <div className="fullscreen-map-layout">
          <MapSidebar
            totalCount={scrapedData.length}
            filteredCount={filteredProperties.length}
            sqftCount={sqftCount}
            filters={mapFilters}
            onFiltersChange={setMapFilters}
            onBack={() => setCurrentView('builder')}
            onShowStats={() => setShowStats(true)}
          />
          <div className="map-wrapper">
            <MapView
              properties={filteredProperties}
              showStationRoutes={mapFilters.showStationRoutes}
            />
          </div>
          {showStats && (
            <StatsOverlay
              properties={filteredProperties}
              onClose={() => setShowStats(false)}
            />
          )}
        </div>

        <button className="help-fab" onClick={() => setShowHelp(true)} title="Show Help Guide">?</button>
        {showHelp && <HelpOverlay onClose={() => setShowHelp(false)} />}
      </>
    );
  }

  // ---------- BUILDER VIEW ----------
  return (
    <div className="container">
      <header className="glass-panel text-center">
        <h1>Rightmove Query Builder</h1>
        <p className="subtitle">Visually construct and deconstruct Rightmove search parameters</p>
      </header>

      <main className="content">
        <section className="glass-panel input-section">
          <h2>Reverse Engineer URL</h2>
          <p className="help-text">Paste an existing Rightmove search URL here to populate the form</p>
          <input
            type="text"
            placeholder="https://www.rightmove.co.uk/property-for-sale/find.html?..."
            value={urlInput}
            onChange={(e) => {
              setUrlInput(e.target.value);
              parseUrl(e.target.value);
            }}
          />
        </section>

        <section className="glass-panel builder-section" style={{ overflow: 'hidden' }}>
          <div
            onClick={() => setIsConfigOpen(!isConfigOpen)}
            style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', cursor: 'pointer' }}
          >
            <h2 style={{ margin: 0 }}>Parameter Configuration</h2>
            <span style={{
              transform: isConfigOpen ? 'rotate(180deg)' : 'rotate(0deg)',
              transition: 'transform 0.3s ease',
              fontSize: '1.2rem',
              color: '#94a3b8'
            }}>
              ▼
            </span>
          </div>

          <div style={{
            maxHeight: isConfigOpen ? '5000px' : '0px',
            opacity: isConfigOpen ? 1 : 0,
            overflow: 'hidden',
            transition: 'all 0.4s ease-in-out',
            marginTop: isConfigOpen ? '1.5rem' : '0'
          }}>
            <div className="grid-container">
              {/* Location */}
              <div className="form-group grid-full">
                <h3>Location</h3>
                <div className="grid-row">
                  <div>
                    <label>Search Location (Visual)</label>
                    <input type="text" value={params.searchLocation} onChange={e => updateParam('searchLocation', e.target.value)} placeholder="e.g. London" />
                  </div>
                  <div>
                    <label>Location Identifier (Technical)</label>
                    <input type="text" value={params.locationIdentifier} onChange={e => updateParam('locationIdentifier', e.target.value)} placeholder="e.g. REGION^87490" />
                  </div>
                  <div>
                    <label>Search Radius (Miles)</label>
                    <select value={params.radius} onChange={e => updateParam('radius', e.target.value)}>
                      <option value="0.0">This area only</option>
                      <option value="0.25">Within ¼ mile</option>
                      <option value="0.5">Within ½ mile</option>
                      <option value="1.0">Within 1 mile</option>
                      <option value="3.0">Within 3 miles</option>
                      <option value="5.0">Within 5 miles</option>
                      <option value="10.0">Within 10 miles</option>
                      <option value="20.0">Within 20 miles</option>
                    </select>
                  </div>
                </div>
              </div>

              {/* Financial */}
              <div className="form-group">
                <h3>Financial</h3>
                <div className="grid-row-2">
                  <div>
                    <label>Min Price (£)</label>
                    <input type="number" step="10000" value={params.minPrice} onChange={e => updateParam('minPrice', e.target.value)} placeholder="Min" />
                  </div>
                  <div>
                    <label>Max Price (£)</label>
                    <input type="number" step="10000" value={params.maxPrice} onChange={e => updateParam('maxPrice', e.target.value)} placeholder="Max" />
                  </div>
                </div>
              </div>

              {/* Size */}
              <div className="form-group">
                <h3>Size</h3>
                <div className="grid-row-2">
                  <div>
                    <label>Min Beds</label>
                    <input type="number" min="0" value={params.minBedrooms} onChange={e => updateParam('minBedrooms', e.target.value)} placeholder="Min" />
                  </div>
                  <div>
                    <label>Max Beds</label>
                    <input type="number" min="0" value={params.maxBedrooms} onChange={e => updateParam('maxBedrooms', e.target.value)} placeholder="Max" />
                  </div>
                  <div>
                    <label>Min Baths</label>
                    <input type="number" min="0" value={params.minBathrooms} onChange={e => updateParam('minBathrooms', e.target.value)} placeholder="Min" />
                  </div>
                  <div>
                    <label>Max Baths</label>
                    <input type="number" min="0" value={params.maxBathrooms} onChange={e => updateParam('maxBathrooms', e.target.value)} placeholder="Max" />
                  </div>
                </div>
              </div>
            </div>

            <div className="grid-container">
              {/* Checkboxes: Property Types */}
              <div className="form-group">
                <h3>Property Types</h3>
                <div className="checkbox-grid">
                  {PROPERTY_TYPES.map(type => (
                    <label key={type.value} className="checkbox-label">
                      <input
                        type="checkbox"
                        checked={params.propertyTypes.includes(type.value)}
                        onChange={() => toggleArrayItem('propertyTypes', type.value)}
                      />
                      <span>{type.label}</span>
                    </label>
                  ))}
                </div>
              </div>

              {/* Checkboxes: Must Have & Tenure */}
              <div className="form-group">
                <h3>Must Haves</h3>
                <div className="checkbox-grid">
                  {MUST_HAVES.map(type => (
                    <label key={type.value} className="checkbox-label">
                      <input
                        type="checkbox"
                        checked={params.mustHave.includes(type.value)}
                        onChange={() => toggleArrayItem('mustHave', type.value)}
                      />
                      <span>{type.label}</span>
                    </label>
                  ))}
                </div>

                <h3 style={{ marginTop: '1.5rem' }}>Tenure</h3>
                <div className="checkbox-grid">
                  {TENURE_TYPES.map(type => (
                    <label key={type.value} className="checkbox-label">
                      <input
                        type="checkbox"
                        checked={params.tenureTypes.includes(type.value)}
                        onChange={() => toggleArrayItem('tenureTypes', type.value)}
                      />
                      <span>{type.label}</span>
                    </label>
                  ))}
                </div>
              </div>
            </div>

            <div className="grid-container" style={{ marginTop: '1.5rem' }}>
              <div className="form-group">
                <h3>Filters</h3>
                <div className="grid-row-2">
                  <div>
                    <label>Added to Site</label>
                    <select value={params.maxDaysSinceAdded} onChange={e => updateParam('maxDaysSinceAdded', e.target.value)}>
                      {MAX_DAYS.map(d => <option key={d.value} value={d.value}>{d.label}</option>)}
                    </select>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', paddingTop: '1.5rem' }}>
                    <label className="checkbox-label">
                      <input
                        type="checkbox"
                        checked={params._includeSSTC}
                        onChange={(e) => updateParam('_includeSSTC', e.target.checked)}
                      />
                      <span>Include Sold Subject to Contract (SSTC)</span>
                    </label>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </section>
      </main>

      <footer className="glass-panel output-section">
        {/* Always show Single URL Section */}
        <div className="single-output">
          <div className="output-header">
            <h2>Generated URL</h2>
            <div style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
              <label className="checkbox-label" style={{ background: 'transparent', border: 'none', padding: 0 }}>
                <input type="checkbox" checked={autoDownload} onChange={e => setAutoDownload(e.target.checked)} />
                <span style={{ fontSize: '0.85rem' }}>Download data locally</span>
              </label>
              <div style={{ display: 'flex', gap: '0.5rem' }}>
                <button
                  onClick={() => document.getElementById('file-upload')?.click()}
                  style={{ padding: '0.4rem 0.8rem', fontSize: '0.85rem', background: 'rgba(255,255,255,0.05)', border: '1px solid rgba(255,255,255,0.1)' }}
                >
                  📁 Upload JSON
                </button>
                <button
                  onClick={() => handleRunScraper([generatedUrl])}
                  disabled={isScraping}
                  style={{ backgroundColor: isScraping ? 'transparent' : 'rgba(16, 185, 129, 0.2)', color: '#34d399', borderColor: '#059669' }}
                >
                  {isScraping
                    ? (scrapeProgress ? `Scraping (${scrapeProgress.current}/${scrapeProgress.total})...` : 'Scraping...')
                    : 'Run Scraper Pipeline'}
                </button>
                <button onClick={handleCopy} className={copied ? 'copied' : ''}>
                  {copied ? 'Copied!' : 'Copy Link'}
                </button>
              </div>
            </div>
          </div>
          <div className="url-display">
            {generatedUrl}
          </div>
        </div>

        {/* Optional Batch Outcode Section */}
        {params.locationIdentifier.includes('OUTCODE^') && (
          <div className="batch-output-optional">
            <div className="batch-divider">
              <span>OR</span>
            </div>
            <div className="output-header">
              <h2 style={{ color: '#60a5fa' }}>Advanced: Batch Outcode Generation</h2>
            </div>
            <p className="help-text" style={{ color: '#bfdbfe', marginBottom: '1rem' }}>Generate multiple unique search URLs for different outcodes using the current parameters.</p>

            <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
              <textarea
                rows={2}
                placeholder="Enter outcodes separated by commas (e.g. AL8, SG1, N1)"
                value={batchOutcodes}
                onChange={(e) => setBatchOutcodes(e.target.value)}
                style={{ width: '100%', padding: '0.75rem', borderRadius: '4px', background: 'rgba(0, 0, 0, 0.2)', border: '1px solid rgba(255, 255, 255, 0.1)', color: 'white' }}
              />

              <button
                onClick={generateBatchUrls}
                disabled={isGeneratingBatch || !batchOutcodes.trim()}
                style={{ alignSelf: 'flex-start' }}
              >
                {isGeneratingBatch ? 'Generating Batch URLs...' : 'Generate Batch URLs'}
              </button>
            </div>

            {batchUrls.length > 0 && (
              <div style={{ marginTop: '1.5rem' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                  <h4 style={{ margin: 0, color: '#f8fafc' }}>Generated Search URLs ({batchUrls.length})</h4>
                  <div style={{ display: 'flex', gap: '0.5rem' }}>
                    <button
                      onClick={() => handleRunScraper(batchUrls)}
                      disabled={isScraping}
                      style={{ padding: '0.4rem 0.8rem', fontSize: '0.9rem', backgroundColor: isScraping ? 'transparent' : 'rgba(16, 185, 129, 0.2)', color: '#34d399', borderColor: '#059669' }}
                    >
                      {isScraping
                        ? (scrapeProgress ? `Scraping (${scrapeProgress.current}/${scrapeProgress.total})...` : 'Scraping...')
                        : 'Run Batch Pipeline'}
                    </button>
                    <button
                      onClick={handleCopyBatch}
                      className={batchCopied ? 'copied' : ''}
                      style={{ padding: '0.4rem 0.8rem', fontSize: '0.9rem' }}
                    >
                      {batchCopied ? 'Copied All!' : 'Copy All URLs'}
                    </button>
                  </div>
                </div>
                <textarea
                  readOnly
                  rows={4}
                  value={batchUrls.join('\n')}
                  style={{ width: '100%', padding: '0.75rem', borderRadius: '4px', background: 'rgba(0, 0, 0, 0.3)', border: '1px solid rgba(255, 255, 255, 0.1)', fontFamily: 'monospace', fontSize: '0.85rem', whiteSpace: 'pre', color: '#a7f3d0' }}
                />
              </div>
            )}
          </div>
        )}
      </footer>
      <input
        type="file"
        id="file-upload"
        accept=".json"
        onChange={handleFileUpload}
        style={{ display: 'none' }}
      />

      {scrapedData.length > 0 && (
        <div style={{ textAlign: 'center', marginTop: '1rem' }}>
          <button
            onClick={() => setCurrentView('map')}
            style={{ backgroundColor: 'rgba(16, 185, 129, 0.2)', color: '#34d399', borderColor: '#059669', padding: '0.75rem 1.5rem' }}
          >
            🗺️ View {scrapedData.length} Properties on Map
          </button>
        </div>
      )}

      <button className="help-fab" onClick={() => setShowHelp(true)} title="Show Help Guide">?</button>
      {showHelp && <HelpOverlay onClose={() => setShowHelp(false)} />}
    </div>
  );
}

export default App;
