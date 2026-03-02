import { useState } from 'react';

export interface MapFilters {
    maxDaysOnMarket: number | null;
    reducedOnly: boolean;
    maxStationDistance: number | null; // null = no limit
    minSqft: number | null;
    notReducedOnly: boolean;
    minPrice: number | null;
    maxPrice: number | null;
    propertyTypes: string[];
    showStationRoutes: boolean;
}

interface MapSidebarProps {
    totalCount: number;
    filteredCount: number;
    sqftCount: number;
    filters: MapFilters;
    onFiltersChange: (filters: MapFilters) => void;
    onBack: () => void;
    onShowStats: () => void;
}

const DAY_PRESETS = [
    { label: 'Any', value: null },
    { label: '≤ 7d', value: 7 },
    { label: '≤ 14d', value: 14 },
    { label: '≤ 30d', value: 30 },
    { label: '≤ 90d', value: 90 },
];

const STATION_PRESETS = [
    { label: 'Any', value: null },
    { label: '≤ 0.5mi', value: 0.5 },
    { label: '≤ 1mi', value: 1 },
    { label: '≤ 2mi', value: 2 },
];

const PROPERTY_TYPE_PRESETS = [
    'Detached',
    'Semi-Detached',
    'Terraced',
    'End of Terrace',
    'Flat',
    'Bungalow',
    'Mews',
    'Town House',
];


export default function MapSidebar({ totalCount, filteredCount, sqftCount, filters, onFiltersChange, onBack, onShowStats }: MapSidebarProps) {
    const [isTypesCollapsed, setIsTypesCollapsed] = useState(true);

    const update = (partial: Partial<MapFilters>) =>
        onFiltersChange({ ...filters, ...partial });

    const hasActiveFilters =
        filters.maxDaysOnMarket !== null ||
        filters.reducedOnly ||
        filters.notReducedOnly ||
        filters.maxStationDistance !== null ||
        filters.minSqft !== null ||
        filters.minPrice !== null ||
        filters.maxPrice !== null ||
        filters.propertyTypes.length > 0;

    return (
        <aside className="map-sidebar glass-panel">
            <button className="back-button" onClick={onBack}>
                ← Back to Builder
            </button>

            <h2>Map Controls</h2>

            <div className="sidebar-stat" onClick={onShowStats} style={{ cursor: 'pointer' }} title="Click for details">
                <span className="stat-number">{filteredCount}</span>
                <span className="stat-label">
                    {filteredCount === totalCount
                        ? 'Properties on Map'
                        : `of ${totalCount} properties`}
                </span>
                <button
                    className="insights-trigger-btn"
                    onClick={(e) => { e.stopPropagation(); onShowStats(); }}
                >
                    📊 View Market Insights
                </button>
            </div>

            <div className="sidebar-section">
                <h3>Price Range</h3>
                <div className="price-range-grid">
                    <input
                        type="number"
                        className="sidebar-input small"
                        placeholder="Min £"
                        value={filters.minPrice || ''}
                        onChange={(e) => update({ minPrice: e.target.value ? Number(e.target.value) : null })}
                    />
                    <input
                        type="number"
                        className="sidebar-input small"
                        placeholder="Max £"
                        value={filters.maxPrice || ''}
                        onChange={(e) => update({ maxPrice: e.target.value ? Number(e.target.value) : null })}
                    />
                </div>
            </div>

            <div className="sidebar-section">
                <div
                    className="sidebar-section-header"
                    onClick={() => setIsTypesCollapsed(!isTypesCollapsed)}
                    style={{ cursor: 'pointer', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}
                >
                    <h3>Property Type</h3>
                    <span style={{
                        fontSize: '0.8rem',
                        transition: 'transform 0.2s',
                        transform: isTypesCollapsed ? 'rotate(-90deg)' : 'rotate(0deg)'
                    }}>
                        ▼
                    </span>
                </div>

                {!isTypesCollapsed && (
                    <div className="type-grid">
                        {PROPERTY_TYPE_PRESETS.map(type => (
                            <label key={type} className="checkbox-label small">
                                <input
                                    type="checkbox"
                                    checked={filters.propertyTypes.includes(type)}
                                    onChange={() => {
                                        const newTypes = filters.propertyTypes.includes(type)
                                            ? filters.propertyTypes.filter(t => t !== type)
                                            : [...filters.propertyTypes, type];
                                        update({ propertyTypes: newTypes });
                                    }}
                                />
                                <span>{type}</span>
                            </label>
                        ))}
                    </div>
                )}
            </div>

            <div className="sidebar-section">
                <h3>Days on Market</h3>

                <div className="preset-grid">
                    {DAY_PRESETS.map(preset => (
                        <button
                            key={String(preset.value)}
                            className={`preset-btn ${filters.maxDaysOnMarket === preset.value ? 'active' : ''}`}
                            onClick={() => update({ maxDaysOnMarket: preset.value })}
                        >
                            {preset.label}
                        </button>
                    ))}
                </div>
            </div>

            <div className="sidebar-section">
                <h3>Station Distance</h3>
                <div className="preset-grid">
                    {STATION_PRESETS.map(preset => (
                        <button
                            key={String(preset.value)}
                            className={`preset-btn ${filters.maxStationDistance === preset.value ? 'active' : ''}`}
                            onClick={() => update({ maxStationDistance: preset.value })}
                        >
                            {preset.label}
                        </button>
                    ))}
                </div>
            </div>

            <div className="sidebar-section">
                <h3>Min Size (Sq Ft)</h3>
                <input
                    type="number"
                    className="sidebar-input"
                    placeholder="e.g. 800"
                    value={filters.minSqft || ''}
                    onChange={(e) => update({ minSqft: e.target.value ? Number(e.target.value) : null })}
                />
                <p className="sidebar-note">
                    {sqftCount} of {totalCount} ({Math.round((sqftCount / totalCount) * 100)}%) properties have sqft data.
                </p>
            </div>

            <div className="sidebar-section">
                <h3>Price Reductions</h3>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                    <label className="toggle-label">
                        <div
                            className={`toggle-switch ${filters.reducedOnly ? 'on' : ''}`}
                            onClick={() => update({ reducedOnly: !filters.reducedOnly, notReducedOnly: false })}
                        >
                            <div className="toggle-knob" />
                        </div>
                        <span>Reduced only</span>
                    </label>
                    <label className="toggle-label">
                        <div
                            className={`toggle-switch ${filters.notReducedOnly ? 'on' : ''}`}
                            onClick={() => update({ notReducedOnly: !filters.notReducedOnly, reducedOnly: false })}
                        >
                            <div className="toggle-knob" />
                        </div>
                        <span>Not reduced only</span>
                    </label>
                </div>
            </div>

            <div className="sidebar-section">
                <h3>Navigation</h3>
                <label className="toggle-label">
                    <div
                        className={`toggle-switch ${filters.showStationRoutes ? 'on' : ''}`}
                        onClick={() => update({ showStationRoutes: !filters.showStationRoutes })}
                    >
                        <div className="toggle-knob" />
                    </div>
                    <span>Walk route to station</span>
                </label>
            </div>

            {hasActiveFilters && (
                <button
                    className="reset-btn"
                    onClick={() => onFiltersChange({
                        maxDaysOnMarket: null,
                        reducedOnly: false,
                        notReducedOnly: false,
                        maxStationDistance: null,
                        minSqft: null,
                        minPrice: null,
                        maxPrice: null,
                        propertyTypes: [],
                        showStationRoutes: true
                    })}
                >
                    ✕ Reset Filters
                </button>
            )}
        </aside>
    );
}
