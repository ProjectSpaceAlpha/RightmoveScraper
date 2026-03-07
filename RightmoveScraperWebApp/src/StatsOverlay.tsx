import { useState } from 'react';
import {
    PieChart, Pie, Cell, ResponsiveContainer, Tooltip, Legend,
    ScatterChart, Scatter, XAxis, YAxis, ZAxis, CartesianGrid,
    AreaChart, Area
} from 'recharts';
import type { PropertyData } from './MapView';
import { getNormalizedPropertyType } from './constants';

interface StatsOverlayProps {
    properties: PropertyData[];
    onClose: () => void;
}

const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16'];

const CustomScatterTooltip = ({ active, payload, formatCurrency, isSticky, onClose, colorBy }: any) => {
    if ((active && payload && payload.length) || (isSticky && payload && payload.length)) {
        const data = payload[0].payload;
        return (
            <div className="custom-scatter-tooltip glass-panel" style={{
                backgroundColor: '#1e293b',
                border: isSticky ? '2px solid #3b82f6' : '1px solid #475569',
                borderRadius: '8px',
                padding: '12px',
                boxShadow: '0 10px 15px -3px rgba(0, 0, 0, 0.4)',
                pointerEvents: 'auto',
                position: 'relative',
                minWidth: '200px'
            }}>
                {isSticky && (
                    <button
                        onClick={(e) => { e.stopPropagation(); onClose(); }}
                        style={{
                            position: 'absolute',
                            top: '8px',
                            right: '8px',
                            background: 'rgba(255,255,255,0.1)',
                            border: 'none',
                            color: '#cbd5e1',
                            cursor: 'pointer',
                            borderRadius: '50%',
                            width: '20px',
                            height: '20px',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            fontSize: '10px'
                        }}
                    >
                        ✕
                    </button>
                )}
                <p style={{ margin: 0, fontWeight: 'bold', color: '#f8fafc', marginBottom: '6px', fontSize: '0.9rem', maxWidth: '170px' }}>
                    {data.name}
                </p>
                <div style={{ fontSize: '0.85rem', display: 'flex', flexDirection: 'column', gap: '4px' }}>
                    <p style={{ margin: 0, color: '#94a3b8' }}>Price: <span style={{ color: '#10b981', fontWeight: '600' }}>{formatCurrency(data.y)}</span></p>
                    <p style={{ margin: 0, color: '#94a3b8' }}>Size: <span style={{ color: '#3b82f6', fontWeight: '600' }}>{data.x.toLocaleString()} ft²</span></p>
                    {colorBy === 'bed' && <p style={{ margin: 0, color: '#94a3b8' }}>Beds: <span style={{ color: '#fcd34d', fontWeight: '600' }}>{data.beds}</span></p>}
                    {colorBy === 'type' && <p style={{ margin: 0, color: '#94a3b8' }}>Type: <span style={{ color: '#fcd34d', fontWeight: '600' }}>{data.type}</span></p>}
                </div>
                {data.url && (
                    <a
                        href={data.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        style={{
                            display: 'inline-block',
                            marginTop: '10px',
                            color: '#60a5fa',
                            textDecoration: 'none',
                            fontSize: '0.8rem',
                            fontWeight: '600',
                            borderTop: '1px solid #334155',
                            paddingTop: '8px',
                            width: '100%'
                        }}
                        onClick={(e) => e.stopPropagation()}
                    >
                        View on Rightmove →
                    </a>
                )}
            </div>
        );
    }
    return null;
};

export default function StatsOverlay({ properties, onClose }: StatsOverlayProps) {
    const [stickyPoint, setStickyPoint] = useState<any>(null);
    const [hideOutliers, setHideOutliers] = useState(true);
    const [fullscreenChart, setFullscreenChart] = useState<string | null>(null);
    const [scatterColorBy, setScatterColorBy] = useState<'none' | 'bed' | 'type'>('none');
    if (properties.length === 0) {
        return (
            <div className="stats-overlay-backdrop" onClick={onClose}>
                <div className="stats-overlay-content glass-panel" onClick={e => e.stopPropagation()}>
                    <header className="stats-header">
                        <h2>Market Insights</h2>
                        <button className="close-btn" onClick={onClose} title="Close Market Insights">
                            <span style={{ fontSize: '20px', fontWeight: 'bold', lineHeight: 1 }}>✕</span>
                        </button>
                    </header>
                    <div style={{ textAlign: 'center', padding: '3rem', color: '#94a3b8' }}>
                        No property data available for current filters.
                    </div>
                </div>
            </div>
        );
    }

    // --- Data Calculations ---
    const validPrices = properties
        .map(p => typeof p.price === 'string' ? parseFloat(p.price) : p.price)
        .filter((p): p is number => p !== undefined && !isNaN(p));

    const highestPrice = validPrices.length ? Math.max(...validPrices) : 0;
    const lowestPrice = validPrices.length ? Math.min(...validPrices) : 0;
    const avgPrice = validPrices.length ? validPrices.reduce((a, b) => a + b, 0) / validPrices.length : 0;

    // Bedroom Distribution (Pie) - Group small counts
    const bedCounts: Record<string, number> = {};
    properties.forEach(p => {
        const beds = p.bedrooms !== undefined ? String(p.bedrooms) : 'Unknown';
        bedCounts[beds] = (bedCounts[beds] || 0) + 1;
    });

    let bedData = Object.entries(bedCounts)
        .map(([name, value]) => ({ name: `${name} Bed`, value }))
        .sort((a, b) => b.value - a.value);

    if (bedData.length > 6) {
        const mainBeds = bedData.slice(0, 5);
        const otherBeds = bedData.slice(5).reduce((acc, curr) => acc + curr.value, 0);
        bedData = [...mainBeds, { name: 'Other', value: otherBeds }];
    }

    // Property Type Distribution (Pie) - Group small counts
    const typeCounts: Record<string, number> = {};
    properties.forEach(p => {
        const type = getNormalizedPropertyType(p.type || '');
        typeCounts[type] = (typeCounts[type] || 0) + 1;
    });

    let typeData = Object.entries(typeCounts)
        .map(([name, value]) => ({ name, value }))
        .sort((a, b) => b.value - a.value);

    if (typeData.length > 8) {
        const mainTypes = typeData.slice(0, 7);
        const otherTypes = typeData.slice(7).reduce((acc, curr) => acc + curr.value, 0);
        typeData = [...mainTypes, { name: 'Other', value: otherTypes }];
    }

    // Sq Ft vs Price (Scatter)
    const rawScatterData = properties
        .filter(p => p.sqft && p.price && !isNaN(Number(p.sqft)) && !isNaN(Number(p.price)))
        .map(p => ({
            x: Number(p.sqft),
            y: Number(p.price),
            name: p.address,
            url: p.url,
            beds: p.bedrooms !== undefined ? String(p.bedrooms) : 'Unknown',
            type: getNormalizedPropertyType(String(p.type || ''))
        }));

    // IQR Outlier Detection
    const getFilteredScatterData = () => {
        if (!hideOutliers || rawScatterData.length < 4) return rawScatterData;

        const values = [...rawScatterData].map(d => d.x).sort((a, b) => a - b);
        const q1 = values[Math.floor(values.length * 0.25)];
        const q3 = values[Math.floor(values.length * 0.75)];
        const iqr = q3 - q1;
        const upperLimit = q3 + (iqr * 1.5);
        const lowerLimit = Math.max(0, q1 - (iqr * 1.5));

        return rawScatterData.filter(d => d.x >= lowerLimit && d.x <= upperLimit);
    };

    const scatterData = getFilteredScatterData();

    // Calculate dynamic XAxis domain (min sqft rounded down to nearest 50)
    const minSqft = scatterData.length > 0 ? Math.min(...scatterData.map(d => d.x)) : 0;
    const roundedMinSqft = Math.max(0, Math.floor(minSqft / 50) * 50);

    // Days on Market Distribution (Area) - Bucketing
    const daysCounts: Record<string, number> = {
        '0-7d': 0, '8-14d': 0, '15-30d': 0, '31-60d': 0, '61-90d': 0, '91d+': 0
    };
    properties.forEach(p => {
        const d = Number(p.days_on_market) || 0;
        if (d <= 7) daysCounts['0-7d']++;
        else if (d <= 14) daysCounts['8-14d']++;
        else if (d <= 30) daysCounts['15-30d']++;
        else if (d <= 60) daysCounts['31-60d']++;
        else if (d <= 90) daysCounts['61-90d']++;
        else daysCounts['91d+']++;
    });
    const daysData = Object.entries(daysCounts).map(([range, count]) => ({ range, count }));

    // Avg Price per Sq Ft
    const sqftProps = properties.filter(p => p.sqft && p.price && Number(p.sqft) > 0);
    const avgPricePerSqFt = sqftProps.length
        ? sqftProps.reduce((sum, p) => sum + (Number(p.price) / Number(p.sqft)), 0) / sqftProps.length
        : 0;

    const formatCurrency = (val: number) => `£${val.toLocaleString('en-GB', { maximumFractionDigits: 0 })}`;

    return (
        <div className="stats-overlay-backdrop" onClick={onClose}>
            <div className={`stats-overlay-content glass-panel ${fullscreenChart ? 'has-fullscreen' : ''}`} onClick={e => e.stopPropagation()}>
                <header className="stats-header">
                    <div>
                        <h2>Market Insights</h2>
                        <p className="subtitle">Analyzing {properties.length} properties in current view</p>
                    </div>
                    <button className="close-btn" onClick={onClose} title="Close Market Insights">
                        <span style={{ fontSize: '20px', fontWeight: 'bold', lineHeight: 1 }}>✕</span>
                    </button>
                </header>

                <div className="stats-grid">
                    {/* Key Metrics */}
                    <div className="stats-card key-metrics">
                        <div className="metric">
                            <span className="label">Highest Price</span>
                            <span className="value">{formatCurrency(highestPrice)}</span>
                        </div>
                        <div className="metric">
                            <span className="label">Average Price</span>
                            <span className="value">{formatCurrency(avgPrice)}</span>
                        </div>
                        <div className="metric">
                            <span className="label">Lowest Price</span>
                            <span className="value">{formatCurrency(lowestPrice)}</span>
                        </div>
                        <div className="metric accent">
                            <span className="label">Avg £ / Sq Ft</span>
                            <span className="value">{formatCurrency(avgPricePerSqFt)}</span>
                        </div>
                    </div>

                    {/* Bedroom Distribution Pie */}
                    <div className={`stats-card ${fullscreenChart === 'bed' ? 'fullscreen' : ''}`}>
                        <div className="chart-header">
                            <h3>Bedroom Distribution</h3>
                            <button className="expand-btn" onClick={() => setFullscreenChart(fullscreenChart === 'bed' ? null : 'bed')}>
                                {fullscreenChart === 'bed' ? '⤓ Minimize' : '⤢ Fullscreen'}
                            </button>
                        </div>
                        <div className="chart-container">
                            <ResponsiveContainer width="99%" height={fullscreenChart === 'bed' ? "100%" : 250}>
                                <PieChart>
                                    <Pie
                                        data={bedData}
                                        cx="50%"
                                        cy="50%"
                                        innerRadius={60}
                                        outerRadius={80}
                                        paddingAngle={5}
                                        dataKey="value"
                                    >
                                        {bedData.map((_, index) => (
                                            <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                                        ))}
                                    </Pie>
                                    <Tooltip
                                        contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '8px' }}
                                        itemStyle={{ color: '#f8fafc' }}
                                    />
                                    <Legend verticalAlign="bottom" height={36} />
                                </PieChart>
                            </ResponsiveContainer>
                        </div>
                    </div>

                    {/* Property Type Pie */}
                    <div className={`stats-card ${fullscreenChart === 'type' ? 'fullscreen' : ''}`}>
                        <div className="chart-header">
                            <h3>Property Types</h3>
                            <button className="expand-btn" onClick={() => setFullscreenChart(fullscreenChart === 'type' ? null : 'type')}>
                                {fullscreenChart === 'type' ? '⤓ Minimize' : '⤢ Fullscreen'}
                            </button>
                        </div>
                        <div className="chart-container">
                            <ResponsiveContainer width="99%" height={fullscreenChart === 'type' ? "100%" : 250}>
                                <PieChart>
                                    <Pie
                                        data={typeData}
                                        cx="50%"
                                        cy="50%"
                                        outerRadius={80}
                                        labelLine={false}
                                        dataKey="value"
                                    >
                                        {typeData.map((_, index) => (
                                            <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                                        ))}
                                    </Pie>
                                    <Tooltip
                                        contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '8px' }}
                                        itemStyle={{ color: '#f8fafc' }}
                                    />
                                    <Legend verticalAlign="bottom" height={36} iconType="circle" />
                                </PieChart>
                            </ResponsiveContainer>
                        </div>
                    </div>

                    {/* Days on Market Area */}
                    <div className={`stats-card wide ${fullscreenChart === 'age' ? 'fullscreen' : ''}`}>
                        <div className="chart-header">
                            <h3>Listing Age (Bucketed)</h3>
                            <button className="expand-btn" onClick={() => setFullscreenChart(fullscreenChart === 'age' ? null : 'age')}>
                                {fullscreenChart === 'age' ? '⤓ Minimize' : '⤢ Fullscreen'}
                            </button>
                        </div>
                        <div className="chart-container">
                            <ResponsiveContainer width="99%" height={fullscreenChart === 'age' ? "100%" : 250}>
                                <AreaChart data={daysData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                                    <defs>
                                        <linearGradient id="colorCount" x1="0" y1="0" x2="0" y2="1">
                                            <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.8} />
                                            <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                                        </linearGradient>
                                    </defs>
                                    <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                                    <XAxis dataKey="range" stroke="#94a3b8" />
                                    <YAxis stroke="#94a3b8" />
                                    <Tooltip
                                        contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '8px' }}
                                        itemStyle={{ color: '#f8fafc' }}
                                    />
                                    <Area type="monotone" dataKey="count" stroke="#3b82f6" fillOpacity={1} fill="url(#colorCount)" />
                                </AreaChart>
                            </ResponsiveContainer>
                        </div>
                    </div>

                    {/* Price vs Sq Ft Scatter */}
                    <div className={`stats-card wide ${fullscreenChart === 'scatter' ? 'fullscreen' : ''}`}>
                        <div className="chart-header">
                            <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                                <h3 style={{ margin: 0 }}>Price vs Square Footage</h3>
                                <label style={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    gap: '8px',
                                    fontSize: '0.75rem',
                                    cursor: 'pointer',
                                    color: hideOutliers ? '#3b82f6' : '#94a3b8',
                                    background: 'rgba(59, 130, 246, 0.05)',
                                    padding: '4px 8px',
                                    borderRadius: '4px',
                                    border: '1px solid rgba(59, 130, 246, 0.1)'
                                }}>
                                    <input
                                        type="checkbox"
                                        checked={hideOutliers}
                                        onChange={(e) => setHideOutliers(e.target.checked)}
                                        style={{ margin: 0, cursor: 'pointer' }}
                                    />
                                    Hide Outliers
                                </label>
                                <select 
                                    className="sidebar-input small" 
                                    style={{ padding: '4px 8px', fontSize: '0.75rem', width: 'auto', background: 'rgba(15, 23, 42, 0.6)' }}
                                    value={scatterColorBy}
                                    onChange={(e) => setScatterColorBy(e.target.value as any)}
                                >
                                    <option value="none">Color: None</option>
                                    <option value="bed">Color: Bedrooms</option>
                                    <option value="type">Color: Property Type</option>
                                </select>
                            </div>
                            <button className="expand-btn" onClick={() => setFullscreenChart(fullscreenChart === 'scatter' ? null : 'scatter')}>
                                {fullscreenChart === 'scatter' ? '⤓ Minimize' : '⤢ Fullscreen'}
                            </button>
                        </div>
                        <div className="chart-container">
                            <ResponsiveContainer width="99%" height={fullscreenChart === 'scatter' ? "100%" : 250}>
                                <ScatterChart
                                    margin={{ top: 20, right: 20, bottom: 20, left: 20 }}
                                    onClick={() => setStickyPoint(null)}
                                >
                                    <CartesianGrid stroke="#334155" />
                                    <XAxis 
                                        type="number" 
                                        dataKey="x" 
                                        name="Sq Ft" 
                                        unit="ft²" 
                                        stroke="#94a3b8" 
                                        domain={[roundedMinSqft, 'auto']}
                                        allowDataOverflow={true}
                                    />
                                    <YAxis type="number" dataKey="y" name="Price" unit="£" stroke="#94a3b8" tickFormatter={(value) => `£${value / 1000}k`} />
                                    <ZAxis type="number" range={[50, 400]} />
                                    <Tooltip
                                        cursor={{ strokeDasharray: '3 3', stroke: '#94a3b8' }}
                                        content={
                                            <CustomScatterTooltip
                                                formatCurrency={formatCurrency}
                                                isSticky={!!stickyPoint}
                                                onClose={() => setStickyPoint(null)}
                                                colorBy={scatterColorBy}
                                            />
                                        }
                                        {...(stickyPoint ? {
                                            active: true,
                                            payload: [{ payload: stickyPoint }],
                                            coordinate: { x: stickyPoint.cx, y: stickyPoint.cy }
                                        } : {})}
                                        wrapperStyle={{ pointerEvents: 'auto', outline: 'none' }}
                                        offset={20}
                                        isAnimationActive={false}
                                    />
                                    {fullscreenChart === 'scatter' && scatterColorBy !== 'none' && (
                                        <Legend 
                                            verticalAlign="bottom" 
                                            height={36}
                                            content={() => {
                                                const activeData = scatterColorBy === 'bed' ? bedData : typeData;
                                                return (
                                                    <div style={{ display: 'flex', justifyContent: 'center', gap: '16px', flexWrap: 'wrap', paddingTop: '16px' }}>
                                                        {activeData.map((entry, index) => (
                                                            <div key={entry.name} style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                                                <div style={{ width: '12px', height: '12px', borderRadius: '50%', backgroundColor: COLORS[index % COLORS.length] }} />
                                                                <span style={{ color: '#f8fafc', fontSize: '12px' }}>{entry.name}</span>
                                                            </div>
                                                        ))}
                                                    </div>
                                                );
                                            }}
                                        />
                                    )}
                                    <Scatter
                                        name="Properties"
                                        data={scatterData}
                                        fill="#10b981"
                                        style={{ cursor: 'pointer' }}
                                        isAnimationActive={false}
                                        onClick={(data, _, e) => {
                                            e.stopPropagation();
                                            setStickyPoint(data);
                                        }}
                                    >
                                        {scatterData.map((entry, index) => {
                                            let cellColor = '#10b981'; // default emerald

                                            if (scatterColorBy === 'bed') {
                                                const bedName = `${entry.beds} Bed`;
                                                const targetIndex = bedData.findIndex(d => d.name === bedName);
                                                if (targetIndex !== -1) {
                                                    cellColor = COLORS[targetIndex % COLORS.length];
                                                } else {
                                                    cellColor = '#64748b'; // unknown
                                                }
                                            } else if (scatterColorBy === 'type') {
                                                const targetIndex = typeData.findIndex(d => d.name === entry.type);
                                                if (targetIndex !== -1) {
                                                    cellColor = COLORS[targetIndex % COLORS.length];
                                                } else {
                                                    cellColor = '#64748b'; // unknown
                                                }
                                            }

                                            return <Cell key={`cell-${index}`} fill={cellColor} />;
                                        })}
                                    </Scatter>
                                </ScatterChart>
                            </ResponsiveContainer>
                        </div>
                    </div>

                </div>
            </div>
        </div>
    );
}
