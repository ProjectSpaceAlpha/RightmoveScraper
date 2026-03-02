/// <reference types="google.maps" />
import { useState, useCallback, useEffect, useRef } from 'react';
import {
    APIProvider,
    Map,
    AdvancedMarker,
    InfoWindow,
    useMap,
} from '@vis.gl/react-google-maps';
import { GOOGLE_MAPS_API_KEY, GOOGLE_MAPS_MAP_ID } from './config';

export interface PropertyData {
    id?: string | number;
    url?: string;
    price?: number | string;
    bedrooms?: number | string;
    type?: string;
    address?: string;
    latitude?: number | string;
    longitude?: number | string;
    list_date?: string;
    reduction_date?: string;
    update_reason?: string;
    days_on_market?: number | string;
    days_since_reduction?: number | string;
    days_to_reduce?: number | string;
    nearest_station_name?: string;
    nearest_station_distance?: number | string;
    sqft?: number | string;
    image_url?: string;
    scraped_at?: string;
    [key: string]: any;
}

interface MapViewProps {
    properties: PropertyData[];
    showStationRoutes: boolean;
}

interface RouteCacheEntry {
    distance: string;
    duration: string;
    path: google.maps.LatLng[];
}

function formatPrice(price: number | string | undefined): string {
    if (price === undefined || price === null || price === '') return 'N/A';
    const num = typeof price === 'string' ? parseFloat(price) : price;
    if (isNaN(num)) return 'N/A';
    return `£${num.toLocaleString('en-GB')}`;
}

function formatDistance(distance: number | string | undefined): string {
    if (distance === undefined || distance === null || distance === '') return 'N/A';
    const num = typeof distance === 'string' ? parseFloat(distance) : distance;
    if (isNaN(num)) return 'N/A';
    return num.toFixed(2);
}

/** Fits the map bounds to all valid markers once */
function AutoFitBounds({ properties }: { properties: PropertyData[] }) {
    const map = useMap();
    const hasFitted = useRef(false);

    useEffect(() => {
        if (!map || hasFitted.current) return;

        const validPoints = properties
            .filter(p => p.latitude && p.longitude && !isNaN(Number(p.latitude)) && !isNaN(Number(p.longitude)))
            .map(p => ({ lat: Number(p.latitude), lng: Number(p.longitude) }));

        if (validPoints.length > 0) {
            const bounds = new google.maps.LatLngBounds();
            validPoints.forEach(pt => bounds.extend(pt));
            map.fitBounds(bounds, { top: 40, right: 40, bottom: 40, left: 40 });
            hasFitted.current = true;
        }
    }, [map, properties]);

    return null;
}


/** Component to handle walking directions from property to station using Routes API v2 */
function WalkingRoute({
    property,
    onRouteCalculated,
    cache
}: {
    property: PropertyData;
    onRouteCalculated: (distance: string, duration: string) => void;
    cache: React.MutableRefObject<Record<string, RouteCacheEntry>>;
}) {
    const map = useMap();
    const polylineRef = useRef<google.maps.Polyline | null>(null);

    useEffect(() => {
        if (!map || !property.nearest_station_name || !property.latitude || !property.longitude) return;

        let cancelled = false;
        const propertyId = property.id ? String(property.id) : `${property.latitude},${property.longitude}`;

        // Check if route is already in cache
        if (cache.current[propertyId]) {
            const cached = cache.current[propertyId];
            onRouteCalculated(cached.distance, cached.duration);

            if (polylineRef.current) {
                polylineRef.current.setMap(null);
            }

            polylineRef.current = new google.maps.Polyline({
                path: cached.path,
                geodesic: true,
                strokeColor: '#3b82f6',
                strokeOpacity: 0.7,
                strokeWeight: 5,
                map: map
            });
            return () => {
                if (polylineRef.current) {
                    polylineRef.current.setMap(null);
                    polylineRef.current = null;
                }
            };
        }

        const fetchRoute = async () => {
            try {
                const response = await fetch('https://routes.googleapis.com/directions/v2:computeRoutes', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Goog-Api-Key': GOOGLE_MAPS_API_KEY,
                        'X-Goog-FieldMask': 'routes.distanceMeters,routes.duration,routes.polyline.encodedPolyline'
                    },
                    body: JSON.stringify({
                        origin: {
                            location: {
                                latLng: {
                                    latitude: Number(property.latitude),
                                    longitude: Number(property.longitude)
                                }
                            }
                        },
                        destination: {
                            address: `${property.nearest_station_name}, UK`
                        },
                        travelMode: 'WALK'
                    })
                });

                const data = await response.json();
                if (!cancelled && data.routes && data.routes.length > 0) {
                    const route = data.routes[0];
                    const encodedPolyline = route.polyline.encodedPolyline;

                    const durationSeconds = parseInt(route.duration);
                    const durationMinutes = Math.round(durationSeconds / 60);
                    const durationText = `${durationMinutes} mins`;

                    const distanceMiles = (route.distanceMeters / 1609.34).toFixed(2);
                    const distanceText = `${distanceMiles} mi`;

                    onRouteCalculated(distanceText, durationText);
                    const path = google.maps.geometry.encoding.decodePath(encodedPolyline);

                    if (polylineRef.current) {
                        polylineRef.current.setMap(null);
                    }

                    polylineRef.current = new google.maps.Polyline({
                        path,
                        geodesic: true,
                        strokeColor: '#3b82f6',
                        strokeOpacity: 0.7,
                        strokeWeight: 5,
                        map: map
                    });

                    cache.current[propertyId] = {
                        distance: distanceText,
                        duration: durationText,
                        path
                    };
                }
            } catch (error) {
                if (!cancelled) {
                    console.error('Error fetching route:', error);
                }
            }
        };

        fetchRoute();

        return () => {
            cancelled = true;
            if (polylineRef.current) {
                polylineRef.current.setMap(null);
                polylineRef.current = null;
            }
        };
    }, [map, property, onRouteCalculated, cache]);

    return null;
}

export default function MapView({ properties, showStationRoutes }: MapViewProps) {
    const [selectedId, setSelectedId] = useState<string | null>(null);
    const [routeInfo, setRouteInfo] = useState<{ distance: string; duration: string } | null>(null);
    const routeCache = useRef<Record<string, RouteCacheEntry>>({});

    const validProperties = properties.filter(
        p => p.latitude && p.longitude && !isNaN(Number(p.latitude)) && !isNaN(Number(p.longitude))
    );

    const handleMarkerClick = useCallback((id: string) => {
        setSelectedId(prev => {
            if (prev === id) {
                setRouteInfo(null);
                return null;
            }
            setRouteInfo(null); // Reset while loading new route
            return id;
        });
    }, []);

    const handleRouteCalculated = useCallback((distance: string, duration: string) => {
        setRouteInfo({ distance, duration });
    }, []);

    const defaultCenter = { lat: 52.035, lng: -2.43 };

    return (
        <APIProvider apiKey={GOOGLE_MAPS_API_KEY} libraries={['geometry']}>
            <Map
                defaultCenter={defaultCenter}
                defaultZoom={10}
                mapId={GOOGLE_MAPS_MAP_ID}
                className="map-container"
                gestureHandling="greedy"
                disableDefaultUI={false}
                clickableIcons={false}
            >
                <AutoFitBounds properties={validProperties} />
                {validProperties.map((property, idx) => {
                    const lat = Number(property.latitude);
                    const lng = Number(property.longitude);
                    const key = property.id ? String(property.id) : `${lat}-${lng}-${idx}`;

                    return (
                        <AdvancedMarker
                            key={key}
                            position={{ lat, lng }}
                            onClick={() => handleMarkerClick(key)}
                        />
                    );
                })}

                {selectedId && (() => {
                    const selected = validProperties.find((p, idx) => {
                        const key = p.id ? String(p.id) : `${Number(p.latitude)}-${Number(p.longitude)}-${idx}`;
                        return key === selectedId;
                    });
                    if (!selected) return null;
                    return (
                        <>
                            {showStationRoutes && (
                                <WalkingRoute
                                    property={selected}
                                    onRouteCalculated={handleRouteCalculated}
                                    cache={routeCache}
                                />
                            )}
                            <InfoWindow
                                position={{ lat: Number(selected.latitude), lng: Number(selected.longitude) }}
                                onCloseClick={() => {
                                    setSelectedId(null);
                                    setRouteInfo(null);
                                }}
                            >
                                <div className="popup-content">
                                    {selected.image_url && (
                                        <img src={selected.image_url} alt="Property" className="popup-image" />
                                    )}
                                    <div className="popup-price">{formatPrice(selected.price)}</div>
                                    <div className="popup-type">
                                        {selected.type || 'Property'}
                                        {selected.bedrooms ? ` · ${selected.bedrooms} bed` : ''}
                                        {selected.sqft ? ` · ${selected.sqft} sq ft` : ''}
                                    </div>
                                    <div className="popup-address">{selected.address || 'Address not available'}</div>

                                    <div className="popup-meta">
                                        {selected.nearest_station_distance !== undefined && (
                                            <div className="popup-tag-group">
                                                <span className="popup-tag">🚉 {formatDistance(selected.nearest_station_distance)} mi ({selected.nearest_station_name})</span>
                                                {routeInfo && (
                                                    <span className="popup-tag walking-tag">🚶 {routeInfo.duration} ({routeInfo.distance})</span>
                                                )}
                                            </div>
                                        )}
                                        {selected.days_on_market && (
                                            <span className="popup-tag">📅 {selected.days_on_market}d on market</span>
                                        )}
                                        {selected.update_reason && (
                                            <span className={`popup-tag ${String(selected.update_reason).toLowerCase().includes('reduced') ? 'reduction' : ''}`}>
                                                {String(selected.update_reason).toLowerCase().includes('reduced') ? '🔻' : 'ℹ️'}{' '}
                                                {selected.update_reason}
                                            </span>
                                        )}
                                    </div>

                                    {selected.url && (
                                        <a
                                            href={selected.url}
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            className="popup-link"
                                        >
                                            View on Rightmove →
                                        </a>
                                    )}
                                </div>
                            </InfoWindow>
                        </>
                    );
                })()}
            </Map>
        </APIProvider>
    );
}
