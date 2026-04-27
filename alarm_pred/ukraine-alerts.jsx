import React, { useState, useEffect, useMemo } from 'react';
import { ComposableMap, Geographies, Geography, ZoomableGroup } from 'react-simple-maps';
import { AlertTriangle, Shield, Clock, MapPin, Database, Target, Crosshair, Plane, Zap, ChevronLeft, ChevronRight } from 'lucide-react';

const UKRAINE_TOPO_URL = '/Ukraine-regions.json';
const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL || '').replace(/\/$/, '');
const FORECAST_ENDPOINT = API_BASE_URL ? `${API_BASE_URL}/api/forecast` : '/api/forecast';

const MODEL_CITIES = [
  'Cherkasy', 'Chernihiv', 'Chernivtsi', 'Dnipro', 'Donetsk', 'Ivano-Frankivsk',
  'Kharkiv', 'Kherson', 'Khmelnytskyi', 'Kropyvnytskyi', 'Kyiv', 'Lutsk',
  'Lviv', 'Mykolaiv', 'Odesa', 'Poltava', 'Rivne', 'Sumy', 'Ternopil',
  'Uzhhorod', 'Vinnytsia', 'Zaporizhzhia', 'Zhytomyr'
];

const CITY_NAME_MAP = {
  'Cherkasy': 'Черкаська',
  'Chernihiv': 'Чернігівська',
  'Chernivtsi': 'Чернівецька',
  'Dnipro': 'Дніпропетровська',
  'Donetsk': 'Донецька',
  'Ivano-Frankivsk': 'Івано-Франківська',
  'Kharkiv': 'Харківська',
  'Kherson': 'Херсонська',
  'Khmelnytskyi': 'Хмельницька',
  'Kropyvnytskyi': 'Кіровоградська',
  'Kyiv': 'Київська',
  'Lutsk': 'Волинська',
  'Lviv': 'Львівська',
  'Mykolaiv': 'Миколаївська',
  'Odesa': 'Одеська',
  'Poltava': 'Полтавська',
  'Rivne': 'Рівненська',
  'Sumy': 'Сумська',
  'Ternopil': 'Тернопільська',
  'Uzhhorod': 'Закарпатська',
  'Vinnytsia': 'Вінницька',
  'Zaporizhzhia': 'Запорізька',
  'Zhytomyr': 'Житомирська'
};

const THREAT_LABELS = {
  ballistic: { label: 'Балістичні ракети / МіГ-31', labelShort: 'БАЛ', icon: Target,    color: '#ef4444' },
  drones:    { label: 'Шахеди / Дрони',             labelShort: 'БПЛА', icon: Plane,     color: '#be123c' },
  cruise:    { label: 'Крилаті ракети',             labelShort: 'КР',   icon: Zap,       color: '#f97316' },
  guided:    { label: 'Керовані бомби',              labelShort: 'КАБ',  icon: Crosshair, color: '#eab308' },
};

const getActiveThreats = (threats) => {
  if (!threats) return [];
  return Object.entries(threats)
    .filter(([, active]) => active)
    .map(([key]) => THREAT_LABELS[key])
    .filter(Boolean);
};

const normalizeApiData = (apiData) => {
  const normalized = { ...apiData, regions_forecast: {} };
  for (const [cityEn, hourlyMap] of Object.entries(apiData.regions_forecast)) {
    const uaName = CITY_NAME_MAP[cityEn] || cityEn;
    normalized.regions_forecast[uaName] = hourlyMap;
  }
  return normalized;
};

const GEO_NAME_MAP = {
  "Vinnyts'ka": 'Вінницька', "Volyns'ka": 'Волинська', "Dnipropetrovs'ka": 'Дніпропетровська',
  "Donets'ka": 'Донецька', "Zhytomyrs'ka": 'Житомирська', "Zakarpats'ka": 'Закарпатська',
  "Zaporiz'ka": 'Запорізька', "Ivano-Frankivs'ka": 'Івано-Франківська', "Kyiv City": 'Київська',
  "Kyivs'ka": 'Київська', "Kirovohrads'ka": 'Кіровоградська', "Luhans'ka": 'Луганська',
  "L'vivs'ka": 'Львівська', "Mykolayivs'ka": 'Миколаївська', "Odes'ka": 'Одеська',
  "Poltavs'ka": 'Полтавська', "Rivnens'ka": 'Рівненська', "Sums'ka": 'Сумська',
  "Ternopil's'ka": 'Тернопільська', "Kharkivs'ka": 'Харківська', "Khersons'ka": 'Херсонська',
  "Khmel'nyts'ka": 'Хмельницька', "Cherkas'ka": 'Черкаська', "Chernihivs'ka": 'Чернігівська',
  "Chernivets'ka": 'Чернівецька',
  "Vinnytska": 'Вінницька', "Volynska": 'Волинська', "Dnipropetrovska": 'Дніпропетровська',
  "Donetska": 'Донецька', "Zhytomyrska": 'Житомирська', "Zakarpatska": 'Закарпатська',
  "Zaporizka": 'Запорізька', "Ivano-Frankivska": 'Івано-Франківська', "Kyiv": 'Київська',
  "Kyivska": 'Київська', "Kirovohradska": 'Кіровоградська', "Luhanska": 'Луганська',
  "Lvivska": 'Львівська', "Mykolaivska": 'Миколаївська', "Odeska": 'Одеська',
  "Poltavska": 'Полтавська', "Rivnenska": 'Рівненська', "Sumska": 'Сумська',
  "Ternopilska": 'Тернопільська', "Kharkivska": 'Харківська', "Khersonska": 'Херсонська',
  "Khmelnytska": 'Хмельницька', "Cherkaska": 'Черкаська', "Chernihivska": 'Чернігівська',
  "Chernivetska": 'Чернівецька',
  "Вінницька область": 'Вінницька', "Волинська область": 'Волинська',
  "Дніпропетровська область": 'Дніпропетровська', "Донецька область": 'Донецька',
  "Житомирська область": 'Житомирська', "Закарпатська область": 'Закарпатська',
  "Запорізька область": 'Запорізька', "Івано-Франківська область": 'Івано-Франківська',
  "Київська область": 'Київська', "Кіровоградська область": 'Кіровоградська',
  "Луганська область": 'Луганська', "Львівська область": 'Львівська',
  "Миколаївська область": 'Миколаївська', "Одеська область": 'Одеська',
  "Полтавська область": 'Полтавська', "Рівненська область": 'Рівненська',
  "Сумська область": 'Сумська', "Тернопільська область": 'Тернопільська',
  "Харківська область": 'Харківська', "Херсонська область": 'Херсонська',
  "Хмельницька область": 'Хмельницька', "Черкаська область": 'Черкаська',
  "Чернігівська область": 'Чернігівська', "Чернівецька область": 'Чернівецька',
  "м. Київ": 'Київська', "місто Київ": 'Київська',
  "Вінницька": 'Вінницька', "Волинська": 'Волинська', "Дніпропетровська": 'Дніпропетровська',
  "Донецька": 'Донецька', "Житомирська": 'Житомирська', "Закарпатська": 'Закарпатська',
  "Запорізька": 'Запорізька', "Івано-Франківська": 'Івано-Франківська', "Кіровоградська": 'Кіровоградська',
  "Луганська": 'Луганська', "Львівська": 'Львівська', "Миколаївська": 'Миколаївська',
  "Одеська": 'Одеська', "Полтавська": 'Полтавська', "Рівненська": 'Рівненська',
  "Сумська": 'Сумська', "Тернопільська": 'Тернопільська', "Харківська": 'Харківська',
  "Херсонська": 'Херсонська', "Хмельницька": 'Хмельницька', "Черкаська": 'Черкаська',
  "Чернігівська": 'Чернігівська', "Чернівецька": 'Чернівецька',
};

const generateMockData = () => {
  const now = new Date();
  const regions_forecast = {};

  MODEL_CITIES.forEach(city => {
    const baseProb = Math.random() * 100;
    const hourlyMap = {};

    for (let h = 0; h < 24; h++) {
      const t = new Date(now.getTime() + h * 3600000);
      const timeKey = t.toLocaleTimeString('uk-UA', { hour: '2-digit', minute: '2-digit' });
      const prob = Math.min(100, Math.max(0, Math.round(baseProb + (Math.random() - 0.5) * 30)));
      const alarm = prob >= 40;
      let threats = null;
      if (prob >= 30) {
        // Generate a random multi-label combination for local mock data.
        const candidates = {
          ballistic: Math.random() > 0.5,
          drones:    Math.random() > 0.4,
          cruise:    Math.random() > 0.6,
          guided:    Math.random() > 0.65,
        };
        // Ensure at least one threat type stays active.
        const anyActive = Object.values(candidates).some(Boolean);
        if (!anyActive) {
          const keys = Object.keys(candidates);
          candidates[keys[Math.floor(Math.random() * keys.length)]] = true;
        }
        threats = candidates;
      }

      hourlyMap[timeKey] = { probability: prob, alarm, threats };
    }

    regions_forecast[city] = hourlyMap;
  });

  return normalizeApiData({
    last_model_train_time: new Date(now.getTime() - 24 * 3600000).toISOString(),
    last_prediction_time: now.toISOString(),
    regions_forecast,
  });
};

const UkraineAlerts = () => {
  const [selectedRegion, setSelectedRegion] = useState(null);
  const [currentTime, setCurrentTime] = useState(new Date());
  const [hoveredRegion, setHoveredRegion] = useState(null);
  const [selectedHour, setSelectedHour] = useState(0);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [geoData, setGeoData] = useState(null);

  useEffect(() => {
    fetch(UKRAINE_TOPO_URL)
      .then(res => res.json())
      .then(topology => {
        setGeoData(topology);
      })
      .catch(err => {
        console.error('Error loading Ukraine topology:', err);
      });
  }, []);

  useEffect(() => {
    setLoading(true);
    setError(null);
    fetch(FORECAST_ENDPOINT)
      .then(res => {
        if (!res.ok) throw new Error(`Server error: HTTP ${res.status}`);
        return res.json();
      })
      .then(raw => {
        setData(normalizeApiData(raw));
        setLoading(false);
      })
      .catch(err => {
        setError(err.message);
        setLoading(false);
      });

    const interval = setInterval(() => {
      fetch(FORECAST_ENDPOINT)
        .then(res => {
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          return res.json();
        })
        .then(raw => {
          setData(normalizeApiData(raw));
          setError(null);
        })
        .catch(err => setError(err.message));
    }, 5 * 60 * 1000);

    return () => clearInterval(interval);
  }, []);

  // Update the live clock once per second.
  useEffect(() => {
    const timer = setInterval(() => setCurrentTime(new Date()), 1000);
    return () => clearInterval(timer);
  }, []);

  const sortedSlots = useMemo(() => {
    if (!data?.regions_forecast) return [];
    const firstRegion = Object.values(data.regions_forecast)[0];
    if (!firstRegion) return [];
    const slots = Object.entries(firstRegion).map(([key, payload]) => ({
      key,
      slotUtc: payload?.slot_datetime_utc || null,
    }));

    // Preferred path: sort by exact UTC datetime from backend.
    const hasExactUtc = slots.every((s) => typeof s.slotUtc === 'string' && s.slotUtc.length > 0);
    if (hasExactUtc) {
      return slots.sort((a, b) => new Date(a.slotUtc) - new Date(b.slotUtc));
    }

    // Backward-compatible fallback for legacy payloads with HH:MM keys only.
    const nowUtcH = new Date().getUTCHours();
    return slots.sort((a, b) => {
      const ha = (parseInt(a.key, 10) - nowUtcH + 24) % 24;
      const hb = (parseInt(b.key, 10) - nowUtcH + 24) % 24;
      return ha - hb;
    });
  }, [data]);

  const visibleSlots = useMemo(() => {
    if (!sortedSlots.length) return [];

    const nowHourUtc = new Date(currentTime);
    nowHourUtc.setUTCMinutes(0, 0, 0);

    const lastPrediction = data?.last_prediction_time ? new Date(data.last_prediction_time) : null;
    if (lastPrediction && !Number.isNaN(lastPrediction.getTime())) {
      const predictionHourUtc = new Date(lastPrediction);
      predictionHourUtc.setUTCMinutes(0, 0, 0);

      // Batch generated at HH:10 is valid for [generation_hour .. generation_hour+23].
      // During the first 10 minutes of next hour, keep only still-valid slots.
      const maxValidHourUtc = new Date(predictionHourUtc.getTime() + 23 * 60 * 60 * 1000);

      return sortedSlots.filter((slot) => {
        if (!slot.slotUtc) return true;
        const slotUtc = new Date(slot.slotUtc);
        return slotUtc >= nowHourUtc && slotUtc <= maxValidHourUtc;
      });
    }

    // Legacy fallback when prediction timestamp is missing.
    return sortedSlots.filter((slot) => {
      if (!slot.slotUtc) return true;
      const slotUtc = new Date(slot.slotUtc);
      return slotUtc >= nowHourUtc;
    });
  }, [sortedSlots, currentTime, data?.last_prediction_time]);

  useEffect(() => {
    const maxIdx = Math.max(0, visibleSlots.length - 1);
    if (selectedHour > maxIdx) setSelectedHour(maxIdx);
  }, [visibleSlots, selectedHour]);

  const getRegionForecast = (regionName) => {
    if (!data?.regions_forecast?.[regionName]) return null;
    const hourlyMap = data.regions_forecast[regionName];
    const timeKey = visibleSlots[selectedHour]?.key;
    return timeKey ? hourlyMap[timeKey] : Object.values(hourlyMap)[0];
  };

  const getThreatStyle = (probability) => {
    if (probability < 30) return { level: 'safe',    fill: '#bbf7d0', glow: '#16a34a', bg: '#f0fdf4',   border: '#86efac' };
    if (probability < 66) return { level: 'warning', fill: '#fef08a', glow: '#eab308', bg: '#fefce8',   border: '#fef08a' };
    if (probability < 80) return { level: 'orange',  fill: '#fb923c', glow: '#fb923c', bg: '#fff3e0',   border: '#fb923c' };
    return                        { level: 'danger',  fill: '#ef4444', glow: '#ef4444', bg: '#fef2f2',   border: '#ef4444' };
  };

  const mapGeoNameToRegion = (geoName) => {
    if (geoName && (geoName.includes('Київ') || geoName.includes('Kyiv'))) return 'Київська';
    return GEO_NAME_MAP[geoName] || geoName;
  };

  const formatTime = (dateString) => {
    return new Date(dateString).toLocaleString('uk-UA', {
      day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit'
    });
  };

  const formatHourLabel = (hourIdx) => {
    const slot = visibleSlots[hourIdx];
    if (!slot) return '—';

    if (slot.slotUtc) {
      return new Date(slot.slotUtc).toLocaleString('uk-UA', {
        hour: '2-digit', minute: '2-digit',
        timeZone: 'Europe/Kyiv'
      });
    }

    // Legacy fallback when backend has only HH:MM keys.
    const [h, m] = slot.key.split(':').map(Number);
    const kyivH = (h + 3) % 24;
    return `${String(kyivH).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
  };

  // Aggregate country-level metrics for the current selection.
  const stats = useMemo(() => {
    if (!data?.regions_forecast) return { activeAlerts: 0, safe: 0, avgProb: 0 };
    const timeKey = visibleSlots[selectedHour]?.key;
    let activeAlerts = 0, safe = 0, totalProb = 0, count = 0;
    Object.values(data.regions_forecast).forEach(hourlyMap => {
      const forecast = timeKey ? hourlyMap[timeKey] : Object.values(hourlyMap)[0];
      if (!forecast) return;
      if (forecast.alarm) activeAlerts++; else safe++;
      totalProb += forecast.probability;
      count++;
    });
    return { activeAlerts, safe, avgProb: count > 0 ? Math.round(totalProb / count) : 0 };
  }, [data, selectedHour, visibleSlots]);

  const selectedHourOffset = useMemo(() => {
    const slot = visibleSlots[selectedHour];
    if (!slot) return null;
    if (slot.slotUtc) {
      const nowHourUtc = new Date(currentTime);
      nowHourUtc.setUTCMinutes(0, 0, 0);
      const slotUtc = new Date(slot.slotUtc);
      return Math.round((slotUtc - nowHourUtc) / (60 * 60 * 1000));
    }
    return selectedHour;
  }, [visibleSlots, selectedHour, currentTime]);

  if (loading || !geoData) {
    return (
      <div style={{
        minHeight: '100vh', background: '#f8fafc', display: 'flex',
        alignItems: 'center', justifyContent: 'center',
        color: '#0f172a', fontFamily: '"Manrope", sans-serif'
      }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{
            width: '60px', height: '60px',
            border: '3px solid #e2e8f0', borderTop: '3px solid #3b82f6',
            borderRadius: '50%', animation: 'spin 1s linear infinite',
            margin: '0 auto 1.5rem'
          }} />
          <p style={{ fontSize: '1.1rem', color: '#64748b' }}>Завантаження прогнозу...</p>
        </div>
        <style>{`@keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }`}</style>
      </div>
    );
  }

  if (error) {
    return (
      <div style={{
        minHeight: '100vh', background: '#f8fafc', display: 'flex',
        alignItems: 'center', justifyContent: 'center',
        fontFamily: '"Manrope", sans-serif'
      }}>
        <div style={{
          textAlign: 'center', padding: '2.5rem', background: '#ffffff',
          borderRadius: '20px', border: '1px solid #fecaca',
          boxShadow: '0 4px 24px rgba(239,68,68,0.08)', maxWidth: '480px'
        }}>
          <div style={{
            width: '64px', height: '64px', borderRadius: '50%',
            background: '#fef2f2', display: 'flex', alignItems: 'center',
            justifyContent: 'center', margin: '0 auto 1.5rem'
          }}>
            <AlertTriangle size={32} style={{ color: '#ef4444' }} />
          </div>
          <h2 style={{ margin: '0 0 0.5rem', fontSize: '1.3rem', fontWeight: 700, color: '#0f172a' }}>
            Помилка зчитування даних
          </h2>
          <p style={{ margin: '0 0 1.5rem', fontSize: '0.9rem', color: '#64748b', lineHeight: 1.6 }}>
            Не вдалося отримати дані з сервера.<br />
            Перевірте що сервер запущений і доступний.
          </p>
          <div style={{
            padding: '0.75rem 1rem', background: '#f8fafc', borderRadius: '8px',
            border: '1px solid #e2e8f0', marginBottom: '1.5rem',
            fontFamily: '"JetBrains Mono", monospace', fontSize: '0.8rem', color: '#ef4444',
            textAlign: 'left', wordBreak: 'break-all'
          }}>
            {error}
          </div>
          <button
            onClick={() => window.location.reload()}
            style={{
              padding: '0.7rem 1.8rem', background: '#3b82f6', color: '#ffffff',
              border: 'none', borderRadius: '10px', fontSize: '0.95rem',
              fontWeight: 600, cursor: 'pointer'
            }}
          >
            Спробувати ще раз
          </button>
        </div>
      </div>
    );
  }

  return (
    <div style={{
      minHeight: '100vh',
      background: '#f8fafc',
      color: '#0f172a',
      fontFamily: '"Manrope", -apple-system, BlinkMacSystemFont, sans-serif',
      overflow: 'auto'
    }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');
        
        @keyframes slideIn { from { transform: translateY(15px); opacity: 0; } to { transform: translateY(0); opacity: 1; } }
        @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
        
        /* Ефекти для карти */
        .region-geo {
          transition: filter 0.25s ease, opacity 0.3s ease;
          cursor: pointer;
        }

        @keyframes glow-danger {
          0%, 100% { filter: drop-shadow(0 0 4px #ef4444) drop-shadow(0 0 10px #ef444455); }
          50%       { filter: drop-shadow(0 0 10px #ef4444) drop-shadow(0 0 22px #ef444488); }
        }
        @keyframes glow-orange {
          0%, 100% { filter: drop-shadow(0 0 4px #fb923c) drop-shadow(0 0 10px #fb923c55); }
          50%       { filter: drop-shadow(0 0 10px #fb923c) drop-shadow(0 0 22px #fb923c88); }
        }
        @keyframes glow-warning {
          0%, 100% { filter: drop-shadow(0 0 4px #eab308) drop-shadow(0 0 10px #eab30855); }
          50%       { filter: drop-shadow(0 0 10px #eab308) drop-shadow(0 0 22px #eab30888); }
        }
        @keyframes glow-safe {
          0%, 100% { filter: drop-shadow(0 0 4px #16a34a) drop-shadow(0 0 10px #16a34a55); }
          50%       { filter: drop-shadow(0 0 10px #16a34a) drop-shadow(0 0 22px #16a34a88); }
        }

        .region-selected-danger  { animation: glow-danger  1.6s ease-in-out infinite; }
        .region-selected-orange  { animation: glow-orange  1.6s ease-in-out infinite; }
        .region-selected-warning { animation: glow-warning 1.8s ease-in-out infinite; }
        .region-selected-safe    { animation: glow-safe    2s   ease-in-out infinite; }
        
        .card { 
          background: #ffffff; 
          border: 1px solid #e2e8f0;
          border-radius: 16px;
          box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
          transition: all 0.3s ease;
        }
        .card:hover { 
          box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
          border-color: #cbd5e1;
        }
        
        .hour-btn {
          transition: all 0.2s ease;
          cursor: pointer;
          user-select: none;
          background: #f1f5f9 !important;
          color: #0f172a !important;
        }
        .hour-btn:hover { background: #e2e8f0 !important; transform: scale(1.05); }
        .hour-btn:active { transform: scale(0.95); }
        
        .slider-track {
          background: #e2e8f0;
        }
        
        .threat-badge {
          animation: fadeIn 0.3s ease;
        }

        /* mobile phone adaptation */
        @media (max-width: 900px) {
          .main-grid {
            display: flex !important;
            flex-direction: column !important;
            gap: 1rem !important;
          }
          .card {
            min-width: 0 !important;
            max-width: 100% !important;
            box-sizing: border-box !important;
          }
        }
        @media (max-width: 600px) {
          body, html {
            font-size: 15px !important;
          }
          .main-grid {
            gap: 0.7rem !important;
          }
          .card {
            padding: 0.7rem !important;
            border-radius: 10px !important;
          }
          header, footer {
            padding-left: 0.5rem !important;
            padding-right: 0.5rem !important;
          }
        }
      `}</style>

      <div style={{ padding: 'clamp(1rem, 3vw, 2rem)', maxWidth: '1800px', margin: '0 auto' }}>
        {/* Header */}
        <header style={{ marginBottom: '1.5rem', animation: 'slideIn 0.5s ease-out' }}>
          <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', flexWrap: 'wrap', gap: '1rem' }}>
            <div>
              <h1 style={{
                fontSize: 'clamp(1.8rem, 4vw, 2.8rem)',
                fontWeight: 800,
                margin: 0,
                color: '#0f172a',
                letterSpacing: '-0.02em'
              }}>
                ПРОГНОЗ ТРИВОГ
              </h1>
              <p style={{
                fontSize: '0.95rem',
                color: '#64748b',
                margin: '0.4rem 0 0 0',
                fontFamily: '"JetBrains Mono", monospace',
                letterSpacing: '0.02em'
              }}>
              </p>
            </div>
            
            <div style={{ 
              display: 'flex', 
              alignItems: 'center', 
              gap: '0.75rem',
              fontFamily: '"JetBrains Mono", monospace',
              fontSize: '0.9rem',
              color: '#475569',
              padding: '0.6rem 1rem',
              background: '#ffffff',
              borderRadius: '10px',
              border: '1px solid #e2e8f0',
              boxShadow: '0 2px 4px rgba(0,0,0,0.02)'
            }}>
              <Clock size={18} style={{ color: '#3b82f6' }} />
              <span>{currentTime.toLocaleTimeString('uk-UA')}</span>
            </div>
          </div>
        </header>

        {/* Hour Selector */}
        <div className="card" style={{ padding: '1.25rem', marginBottom: '1.5rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
              <Target size={20} style={{ color: '#3b82f6' }} />
              <span style={{ fontWeight: 600, fontSize: '1rem' }}>Час прогнозу</span>
            </div>
            <div style={{ 
              fontFamily: '"JetBrains Mono", monospace',
              fontSize: '1.1rem',
              fontWeight: 600,
              color: '#3b82f6',
              padding: '0.4rem 0.8rem',
              background: '#eff6ff',
              borderRadius: '8px'
            }}>
              {formatHourLabel(selectedHour)}
              <span style={{ color: '#94a3b8', marginLeft: '0.5rem', fontSize: '0.85rem' }}>
                ({selectedHourOffset === 0 ? 'зараз' : `+${Math.max(0, selectedHourOffset ?? selectedHour)} год`})
              </span>
            </div>
          </div>
          
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
            <button 
              className="hour-btn"
              onClick={() => setSelectedHour(Math.max(0, selectedHour - 1))}

              style={{
                width: '36px', height: '36px', border: 'none', borderRadius: '8px',
                display: 'flex', alignItems: 'center', justifyContent: 'center'
              }}
            >
              <ChevronLeft size={20} />
            </button>
            
            <div style={{ flex: 1, position: 'relative' }}>
              <div className="slider-track" style={{ 
                position: 'absolute', top: '50%', transform: 'translateY(-50%)',
                left: 0, right: 0, height: '6px', borderRadius: '3px' 
              }} />
              <input
                type="range"
                min="0" max={Math.max(0, visibleSlots.length - 1)}
                value={selectedHour}
                onChange={(e) => setSelectedHour(parseInt(e.target.value))}
                style={{
                  width: '100%', height: '24px', background: 'transparent',
                  appearance: 'none', cursor: 'pointer', position: 'relative', zIndex: 1
                }}
              />
              <style>{`
                input[type="range"]::-webkit-slider-thumb {
                  appearance: none; width: 20px; height: 20px;
                  background: #3b82f6; border-radius: 50%; cursor: pointer;
                  box-shadow: 0 0 10px rgba(59, 130, 246, 0.3);
                }
                input[type="range"]::-moz-range-thumb {
                  width: 20px; height: 20px; background: #3b82f6;
                  border-radius: 50%; cursor: pointer; border: none;
                  box-shadow: 0 0 10px rgba(59, 130, 246, 0.3);
                }
              `}</style>
            </div>
            
            <button 
              className="hour-btn"
              onClick={() => setSelectedHour(Math.min(Math.max(0, visibleSlots.length - 1), selectedHour + 1))}
              style={{
                width: '36px', height: '36px', border: 'none', borderRadius: '8px',
                display: 'flex', alignItems: 'center', justifyContent: 'center'
              }}
            >
              <ChevronRight size={20} />
            </button>
          </div>
          
          {/* Hour labels */}
          <div style={{ 
            display: 'flex', justifyContent: 'space-between', marginTop: '0.5rem',
            fontSize: '0.7rem', color: '#94a3b8', fontFamily: '"JetBrains Mono", monospace'
          }}>
            <span>Зараз</span>
            <span>+6 год</span>
            <span>+12 год</span>
            <span>+18 год</span>
            <span>+24 год</span>
          </div>
        </div>

        {/* Main Grid */}
        <div className="main-grid" style={{ display: 'grid', gridTemplateColumns: '300px 1fr', gap: '1.5rem' }}>
          {/* Left Panel - Stats */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
            {/* Summary Stats */}
            <div className="card" style={{ padding: '1.25rem' }}>
              <h3 style={{ margin: '0 0 1rem 0', fontSize: '0.9rem', fontWeight: 600, color: '#64748b' }}>
                ОГЛЯД СИТУАЦІЇ
              </h3>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                    <div style={{ width: '12px', height: '12px', borderRadius: '50%', background: '#ef4444' }} />
                    <span style={{ fontSize: '0.85rem', color: '#475569' }}>Регіони з тривогою</span>
                  </div>
                  <span style={{ fontWeight: 700, fontSize: '1.1rem', color: '#0f172a' }}>{stats.activeAlerts}</span>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                    <div style={{ width: '12px', height: '12px', borderRadius: '50%', background: '#e2e8f0' }} />
                    <span style={{ fontSize: '0.85rem', color: '#475569' }}>Безпечно</span>
                  </div>
                  <span style={{ fontWeight: 700, fontSize: '1.1rem', color: '#0f172a' }}>{stats.safe}</span>
                </div>
              </div>
              <div style={{ marginTop: '1rem', paddingTop: '1rem', borderTop: '1px solid #e2e8f0' }}>
                <div style={{ fontSize: '0.75rem', color: '#94a3b8', marginBottom: '0.3rem' }}>
                  Середня ймовірність по країні
                </div>
                <div style={{ fontSize: '1.8rem', fontWeight: 800, color: '#3b82f6' }}>
                  {stats.avgProb}%
                </div>
              </div>
            </div>

            {/* Threat Types Legend */}
            <div className="card" style={{ padding: '1.25rem' }}>
              <h3 style={{ margin: '0 0 1rem 0', fontSize: '0.9rem', fontWeight: 600, color: '#64748b' }}>
                ТИПИ ЗАГРОЗ
              </h3>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.6rem' }}>
                {Object.entries(THREAT_LABELS).map(([key, threat]) => {
                  const Icon = threat.icon;
                  return (
                    <div key={key} style={{
                      display: 'flex', alignItems: 'center', gap: '0.6rem',
                      padding: '0.5rem 0.6rem', background: '#f8fafc', borderRadius: '8px',
                      border: '1px solid #e2e8f0'
                    }}>
                      <Icon size={16} style={{ color: threat.color }} />
                      <span style={{ fontSize: '0.8rem', color: '#334155', fontWeight: 500 }}>{threat.label}</span>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Selected Region Details */}
            {selectedRegion && data?.regions_forecast?.[selectedRegion] && (() => {
              const forecast = getRegionForecast(selectedRegion);
              const styles = getThreatStyle(forecast.probability);
              const activeThreats = getActiveThreats(forecast.threats);

              return (
                <div className="card" style={{
                  padding: '1.25rem',
                  background: forecast.probability >= 30 ? styles.bg : '#ffffff',
                  borderColor: forecast.probability >= 30 ? styles.border : '#e2e8f0',
                  animation: 'slideIn 0.3s ease-out'
                }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem', marginBottom: '1rem' }}>
                    <MapPin size={18} style={{ color: forecast.alarm ? styles.fill : '#64748b' }} />
                    <h4 style={{ margin: 0, fontSize: '1rem', fontWeight: 700, color: '#0f172a' }}>
                      {selectedRegion} обл.
                    </h4>
                  </div>

                  <div style={{ marginBottom: '1rem' }}>
                    <div style={{ fontSize: '0.7rem', color: '#64748b', marginBottom: '0.3rem' }}>
                      Ймовірність тривоги
                    </div>
                    <div style={{ fontSize: '2.2rem', fontWeight: 800, color: forecast.probability >= 30 ? styles.fill : '#0f172a' }}>
                      {forecast.probability}%
                    </div>
                  </div>

                  {/* Типи загроз */}
                  {forecast.probability >= 30 && (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.4rem', marginBottom: '1rem' }}>
                      {activeThreats.length > 0 ? activeThreats.map((threat) => {
                        const Icon = threat.icon;
                        return (
                          <div key={threat.labelShort} className="threat-badge" style={{
                            display: 'flex', alignItems: 'center', gap: '0.5rem',
                            padding: '0.5rem 0.8rem', background: '#ffffff',
                            borderRadius: '8px', border: `1px solid ${threat.color}40`
                          }}>
                            <Icon size={16} style={{ color: threat.color }} />
                            <span style={{ fontSize: '0.82rem', fontWeight: 600, color: threat.color }}>
                              {threat.label}
                            </span>
                          </div>
                        );
                      }) : (
                        <div className="threat-badge" style={{
                          display: 'flex', alignItems: 'center', gap: '0.5rem',
                          padding: '0.5rem 0.8rem', background: '#ffffff',
                          borderRadius: '8px', border: '1px solid #fde04740'
                        }}>
                          <AlertTriangle size={16} style={{ color: '#ca8a04' }} />
                          <span style={{ fontSize: '0.82rem', fontWeight: 600, color: '#ca8a04' }}>
                            Тип загрози невизначений
                          </span>
                        </div>
                      )}
                    </div>
                  )}

                  {/* Mini hourly chart */}
                  <div style={{ marginTop: '0.5rem' }}>
                    <div style={{ fontSize: '0.7rem', color: '#94a3b8', marginBottom: '0.5rem' }}>
                      Прогноз на {visibleSlots.length} годин
                    </div>
                    <div style={{ display: 'flex', gap: '2px', height: '40px', alignItems: 'flex-end' }}>
                      {visibleSlots.map((slot, i) => {
                        const key = slot.key;
                        const hourData = data.regions_forecast[selectedRegion]?.[key];
                        const prob = hourData?.probability || 0;
                        const hStyles = getThreatStyle(prob);
                        return (
                          <div
                            key={key}
                            style={{
                              flex: 1, height: `${prob}%`, minHeight: '2px',
                              background: hStyles.fill,
                              opacity: i === selectedHour ? 1 : 0.4,
                              borderRadius: '2px 2px 0 0',
                              transition: 'all 0.2s ease'
                            }}
                            title={`${key}: ${prob}%`}
                          />
                        );
                      })}
                    </div>
                  </div>
                </div>
              );
            })()}

            {/* Model Info */}
            <div className="card" style={{ padding: '1rem' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.75rem' }}>
                <Database size={16} style={{ color: '#3b82f6' }} />
                <span style={{ fontSize: '0.8rem', fontWeight: 600, color: '#64748b' }}>МОДЕЛЬ</span>
              </div>
              <div style={{ fontSize: '0.75rem', color: '#64748b', fontFamily: '"JetBrains Mono", monospace' }}>
                <div style={{ marginBottom: '0.3rem' }}>
                  Навчання: {data?.last_model_train_time ? formatTime(data.last_model_train_time) : '—'}
                </div>
                <div>
                  Прогноз: {data?.last_prediction_time ? formatTime(data.last_prediction_time) : '—'}
                </div>
              </div>
            </div>
          </div>

          {/* Right Panel - Map */}
          <div className="card" style={{ padding: '1.5rem', minHeight: '600px' }}>
            <div style={{ 
              display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem'
            }}>
              <h3 style={{ margin: 0, fontSize: '1.1rem', fontWeight: 700, color: '#0f172a' }}>
                Карта України
              </h3>
              {hoveredRegion && (
                <div style={{
                  fontSize: '0.9rem', color: '#3b82f6', fontWeight: 600,
                  fontFamily: '"JetBrains Mono", monospace', animation: 'fadeIn 0.2s ease-out'
                }}>
                  {hoveredRegion} обл.
                </div>
              )}
            </div>

            <div style={{ position: 'relative', width: '100%', height: 'calc(100% - 50px)' }}>
              <ComposableMap
                projection="geoMercator"
                projectionConfig={{ center: [31.5, 49], scale: 2800 }}
                style={{ width: '100%', height: '100%' }}
              >
                <ZoomableGroup center={[31.5, 49]} zoom={1}>
                  <Geographies geography={geoData}>
                    {({ geographies }) =>
                      geographies.map((geo) => {
                        const geoName = geo.properties?.name || geo.properties?.NAME_1 || geo.properties?.name_uk || '';
                        const regionName = mapGeoNameToRegion(geoName);

                        const forecast = getRegionForecast(regionName);

                        if (!forecast) {
                          return (
                            <Geography
                              key={geo.rsmKey} geography={geo}
                              fill="#f1f5f9" stroke="#cbd5e1" strokeWidth={0.5}
                              style={{ default: { outline: 'none' }, hover: { outline: 'none' }, pressed: { outline: 'none' } }}
                            />
                          );
                        }
                        
                        const styles = getThreatStyle(forecast.probability);
                        const isSelected = selectedRegion === regionName;
                        const isHovered = hoveredRegion === regionName;

                        const selectedClass = isSelected ? `region-selected-${styles.level}` : '';

                        return (
                          <Geography
                            key={geo.rsmKey}
                            geography={geo}
                            className={`region-geo ${selectedClass}`}
                            fill={styles.fill}
                            stroke={isSelected || isHovered ? styles.glow : '#94a3b8'}
                            strokeWidth={isSelected ? 2 : isHovered ? 1.5 : 0.5}
                            style={{
                              default: {
                                outline: 'none',
                                opacity: selectedRegion && !isSelected ? 0.35 : 1,
                                transition: 'opacity 0.3s ease',
                              },
                              hover: {
                                outline: 'none',
                                opacity: 1,
                                filter: `brightness(1.12) drop-shadow(0 0 7px ${styles.glow}) drop-shadow(0 0 14px ${styles.glow}66)`,
                                cursor: 'pointer',
                              },
                              pressed: {
                                outline: 'none',
                                filter: `brightness(1.2) drop-shadow(0 0 12px ${styles.glow})`,
                              }
                            }}
                            onMouseEnter={() => setHoveredRegion(regionName)}
                            onMouseLeave={() => setHoveredRegion(null)}
                            onClick={() => setSelectedRegion(isSelected ? null : regionName)}
                          />
                        );
                      })
                    }
                  </Geographies>
                </ZoomableGroup>
              </ComposableMap>
            </div>

            {/* Map Legend */}
            <div style={{
              marginTop: '1rem', padding: '0.75rem 1rem', background: '#f8fafc',
              borderRadius: '10px', border: '1px solid #e2e8f0',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              gap: '1.5rem', flexWrap: 'wrap', fontSize: '0.8rem'
            }}>
              {[
                { color: '#bbf7d0', label: '0–30% — безпечно' },
                { color: '#fef08a', label: '30–66% — низький ризик' },
                { color: '#fb923c', label: '66–80% — підвищена небезпека' },
                { color: '#ef4444', label: '80–100% — висока небезпека' },
              ].map(({ color, label }) => (
                <div key={label} style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                  <div style={{ width: '12px', height: '12px', borderRadius: '3px', background: color }} />
                  <span style={{ color: '#475569', fontWeight: 500 }}>{label}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Footer */}
        <footer style={{
          marginTop: '2rem', padding: '1rem 0', borderTop: '1px solid #e2e8f0',
          textAlign: 'center', color: '#94a3b8', fontSize: '0.8rem',
          fontFamily: '"JetBrains Mono", monospace'
        }}>
          🇺🇦 Система прогнозування повітряних тривог | 2026
        </footer>
      </div>
    </div>
  );
};

export default UkraineAlerts;
