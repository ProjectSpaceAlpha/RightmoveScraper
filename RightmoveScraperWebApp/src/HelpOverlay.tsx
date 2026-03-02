
// Adding comment to ensure file remains valid if needed, or simply remove if pure TS

interface HelpOverlayProps {
    onClose: () => void;
}

export default function HelpOverlay({ onClose }: HelpOverlayProps) {
    return (
        <div className="stats-overlay-backdrop" onClick={onClose} style={{ zIndex: 3000 }}>
            <div className="stats-overlay-content glass-panel help-content" onClick={e => e.stopPropagation()} style={{ maxWidth: '800px' }}>
                <header className="stats-header">
                    <div>
                        <h2>Application Guide & Help</h2>
                        <p className="subtitle">Everything you need to know about the Rightmove Scraper</p>
                    </div>
                    <button className="close-btn" onClick={onClose} title="Close Help">
                        <span style={{ fontSize: '20px', fontWeight: 'bold', lineHeight: 1 }}>✕</span>
                    </button>
                </header>

                <div className="help-sections">
                    <section className="help-section">
                        <h3>🔍 Using the Query Builder</h3>
                        <ul>
                            <li><strong>Reverse Engineer</strong>: Paste any search result URL from the Rightmove website into the top box. The app will automatically "deconstruct" it into the configuration form.</li>
                            <li><strong>Parameters</strong>: Adjust prices, bedrooms, property types, and more. The "Location Identifier" is unique to Rightmove (e.g., <code>REGION^87490</code> for London).</li>
                            <li><strong>Radius</strong>: Define how far from the center point you want to search.</li>
                        </ul>
                    </section>

                    <section className="help-section">
                        <h3>🕷️ Running the Scraper</h3>
                        <ul>
                            <li><strong>Single Scrape</strong>: Click "Run Scraper Pipeline" to extract data for the current configuration.</li>
                            <li><strong>Batch Mode</strong>: If your search uses an "Outcode" (e.g., AL8), you can enter multiple outcodes (separated by commas) to scrape several areas at once.</li>
                            <li><strong>JSON Export</strong>: Check "Download data locally" to receive a <code>.json</code> file automatically when the scrape finishes.</li>
                        </ul>
                    </section>

                    <section className="help-section">
                        <h3>🗺️ Map & Market Analysis</h3>
                        <ul>
                            <li><strong>Filters</strong>: Use the sidebar on the map to refine your results in real-time.</li>
                            <li><strong>Station Routes</strong>: Toggle "Directions to Station" to see total walking time and routes to the nearest train station for any property.</li>
                            <li><strong>📊 Market Insights</strong>: Open this overlay to see price distributions, property type breakdowns, and square footage value analysis.</li>
                        </ul>
                    </section>

                    <section className="help-section">
                        <h3>💡 Pro Tips</h3>
                        <ul>
                            <li><strong>Square Footage</strong>: Not all listings have square footage. Use the "Price vs Square Footage" scatter plot in Insights to find properties that offer the best value per square foot.</li>
                            <li><strong>Listing Age</strong>: Filter for "Reduced" properties to find sellers who might be more open to negotiation.</li>
                            <li><strong>Data Persistence</strong>: Use the 📁 <strong>Upload JSON</strong> button to reload previous scrapes without waiting for the scraper again.</li>
                        </ul>
                    </section>
                </div>

                <footer style={{ marginTop: '1rem', paddingTop: '1rem', borderTop: '1px solid rgba(255,255,255,0.1)', textAlign: 'center' }}>
                    <button onClick={onClose} style={{ background: 'var(--accent-color)', minWidth: '120px' }}>Got it!</button>
                </footer>
            </div>
        </div>
    );
}
