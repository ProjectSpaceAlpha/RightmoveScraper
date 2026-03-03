import { useState } from 'react';
import {
    PieChart, Pie, Cell, ResponsiveContainer, Tooltip, Legend,
    ScatterChart, Scatter, XAxis, YAxis, ZAxis, CartesianGrid,
    AreaChart, Area
} from 'recharts';
import type { PropertyData } from './MapView';

interface StatsOverlayProps {
    properties: PropertyData[];
    onClose: () => void;
}

const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16'];

const CustomScatterTooltip = ({ active, payload, formatCurrency, isSticky, onClose }: any) => {
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
        const type = p.type || 'Other';
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
            url: p.url
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
            <div className="stats-overlay-content glass-panel" onClick={e => e.stopPropagation()}>
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
                    <div className="stats-card">
                        <h3>Bedroom Distribution</h3>
                        <div className="chart-container">
                            <ResponsiveContainer width="99%" height={250}>
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
                    <div className="stats-card">
                        <h3>Property Types</h3>
                        <div className="chart-container">
                            <ResponsiveContainer width="99%" height={250}>
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
                    <div className="stats-card wide">
                        <h3>Listing Age (Bucketed)</h3>
                        <div className="chart-container">
                            <ResponsiveContainer width="99%" height={250}>
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
                    <div className="stats-card wide">
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
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
                        </div>
                        <div className="chart-container">
                            <ResponsiveContainer width="99%" height={250}>
                                <ScatterChart
                                    margin={{ top: 20, right: 20, bottom: 20, left: 20 }}
                                    onClick={() => setStickyPoint(null)}
                                >
                                    <CartesianGrid stroke="#334155" />
                                    <XAxis type="number" dataKey="x" name="Sq Ft" unit="ft²" stroke="#94a3b8" />
                                    <YAxis type="number" dataKey="y" name="Price" unit="£" stroke="#94a3b8" tickFormatter={(value) => `£${value / 1000}k`} />
                                    <ZAxis type="number" range={[50, 400]} />
                                    <Tooltip
                                        cursor={{ strokeDasharray: '3 3', stroke: '#94a3b8' }}
                                        content={
                                            <CustomScatterTooltip
                                                formatCurrency={formatCurrency}
                                                isSticky={!!stickyPoint}
                                                onClose={() => setStickyPoint(null)}
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
                                    />
                                </ScatterChart>
                            </ResponsiveContainer>
                        </div>
                    </div>

                </div>
            </div>
        </div>
    );
}
