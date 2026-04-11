// =============================================================================
// API INTEGRATION GUIDE - Підключення до бекенду
// =============================================================================
//
// Цей файл описує формат даних, які бекенд повинен повертати для фронтенду.
// Структура відповідає ML моделям з model_training_pipeline.ipynb:
//   - Binary Model  (v1): HistGradientBoosting — прогнозує чи буде тривога (0/1)
//   - Multi-Label   (v2): MultiOutputClassifier(HistGBM) — визначає тип загрози
//       Повертає 4 НЕЗАЛЕЖНИХ бінарних лейбла (не одну цифру!):
//         label_ballistic: 1 якщо балістика або МіГ-31
//         label_drones:    1 якщо шахеди/дрони
//         label_cruise:    1 якщо крилаті ракети
//         label_guided:    1 якщо керовані бомби
//       Один рядок може мати декілька міток одночасно (напр. [1,0,1,0])
//
// =============================================================================

// API Base URL - замініть на адресу вашого бекенду
const API_BASE_URL = '' ;

// =============================================================================
// ФОРМАТ ВІДПОВІДІ API
// =============================================================================

/*
Бекенд повертає JSON з такою структурою:

{
  "last_model_train_time": "2026-04-08T10:30:00Z",  // Коли модель була натренована
  "last_prediction_time": "2026-04-09T14:00:00Z",   // Коли зроблено прогноз

  "regions_forecast": {
    "Kyiv": {
      "12:00": {                    // T+1 (перша прогнозована година)
        "probability": 85,          // v1: ймовірність тривоги (0-100)
        "alarm": true,              // v1: чи буде тривога (threshold з моделі)
        "threats": {                // v2: 4 незалежних лейбла (null якщо alarm=false)
          "ballistic": false,       // label_ballistic
          "drones": true,           // label_drones
          "cruise": false,          // label_cruise
          "guided": false           // label_guided
        }
      },
      "13:00": {                    // T+2
        "probability": 60,
        "alarm": true,
        "threats": {
          "ballistic": false,
          "drones": true,
          "cruise": false,
          "guided": false
        }
      },
      // ... до наступних годин
      "11:00": {
        "probability": 20,
        "alarm": false,
        "threats": null             // null якщо alarm=false — v2 не запускається
      }
    },
    "Kharkiv": { ... },
    // ... всі 23 регіони
  }
}
*/

// =============================================================================
// ТИПИ ЗАГРОЗ (multi-label v2 — 4 незалежних boolean)
// =============================================================================

// v2 повертає НЕ одну цифру, а об'єкт з 4-ма незалежними полями
const THREAT_LABELS = {
  ballistic: "Балістичні ракети / МіГ-31",   // label_ballistic
  drones:    "Шахеди / Дрони",               // label_drones
  cruise:    "Крилаті ракети",               // label_cruise
  guided:    "Керовані бомби (ФАБ)",         // label_guided
};

// Утиліта: масив активних загроз з threats-обʼєкту
function getActiveThreatNames(threats) {
  if (!threats) return [];
  return Object.entries(threats)
    .filter(([, active]) => active)
    .map(([key]) => THREAT_LABELS[key] || key);
}

// =============================================================================
// МІСТА ЯКІ ПРОГНОЗУЄ МОДЕЛЬ
// =============================================================================

const MODEL_CITIES = [
  'Cherkasy',       // Черкаська
  'Chernihiv',      // Чернігівська
  'Chernivtsi',     // Чернівецька
  'Dnipro',         // Дніпропетровська
  'Donetsk',        // Донецька
  'Ivano-Frankivsk',// Івано-Франківська
  'Kharkiv',        // Харківська
  'Kherson',        // Херсонська
  'Khmelnytskyi',   // Хмельницька
  'Kropyvnytskyi',  // Кіровоградська
  'Kyiv',           // Київська
  'Lutsk',          // Волинська
  'Lviv',           // Львівська
  'Mykolaiv',       // Миколаївська
  'Odesa',          // Одеська
  'Poltava',        // Полтавська
  'Rivne',          // Рівненська
  'Sumy',           // Сумська
  'Ternopil',       // Тернопільська
  'Uzhhorod',       // Закарпатська
  'Vinnytsia',      // Вінницька
  'Zaporizhzhia',   // Запорізька
  'Zhytomyr'        // Житомирська
];

// =============================================================================
// ЛОГІКА ВИЗНАЧЕННЯ СТАТУСУ
// =============================================================================

function calculateStatus(probability) {
  if (probability >= 80) return 'danger';   // Червоний - висока небезпека
  if (probability >= 40) return 'warning';  // Жовтий - підвищена увага
  return 'safe';                             // Зелений - низький ризик
}

// Утиліта: людський summary для одного годинного слоту
function summarizeHour(hourData) {
  const names = getActiveThreatNames(hourData.threats);
  if (!hourData.alarm) return "Тривога не прогнозується";
  if (names.length === 0) return "Тривога (тип невідомий)";
  return names.join(" + ");
}

// =============================================================================
// ПРИКЛАД FLASK БЕКЕНДУ
// =============================================================================

/*
# --- Правильний Flask endpoint (відповідає batch_predict.py output) ---
#
# batch_predict.py вже генерує predictions_latest.json у правильному форматі.
# Flask просто читає цей файл і повертає. Нічого рахувати тут не треба.
#
# GET /api/forecast?region=Kyiv  або  /api/forecast?region=all

from flask import Flask, jsonify, request
from flask_cors import CORS
import json
from pathlib import Path

app = Flask(__name__)
CORS(app)

PREDICTIONS_FILE = Path("data/predictions/predictions_latest.json")

@app.route('/api/forecast', methods=['GET'])
def get_forecast():
  region = request.args.get('region', 'all')
  with open(PREDICTIONS_FILE) as f:
    data = json.load(f)
  if region != 'all' and region in data['regions_forecast']:
    data['regions_forecast'] = {region: data['regions_forecast'][region]}
  return jsonify(data)
*/

// =============================================================================
// ІНТЕГРАЦІЯ У ФРОНТЕНД
// =============================================================================

/*
У файлі ukraine-alerts.jsx замініть функцію generateMockData на:

endpoint
*/

const fetchForecast = async (region = 'all') => {
  try {
    const url = `${API_BASE_URL}/api/forecast?region=${encodeURIComponent(region)}`;
    const response = await fetch(url, { method: 'GET' });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const forecastData = await response.json();
    return forecastData;
  } catch (err) {
    console.error('Помилка отримання прогнозу:', err);
    throw err;
  }
};

// В useEffect замість setTimeout з generateMockData:
/*
useEffect(() => {
  setLoading(true);
  fetchForecast()
    .then(data => {
      setData(data);
      setLoading(false);
    })
    .catch(err => {
      setError(err.message);
      setLoading(false);
    });

  // Оновлення кожні 5 хвилин
  const interval = setInterval(() => {
    fetchForecast().then(setData).catch(console.error);
  }, 5 * 60 * 1000);

  return () => clearInterval(interval);
}, []);
*/

// =============================================================================
// CORS НАЛАШТУВАННЯ ДЛЯ FLASK
// =============================================================================

/*
pip install flask-cors

from flask_cors import CORS
app = Flask(__name__)
CORS(app, origins=['http://localhost:3000', 'http://localhost:5173'])
*/

// =============================================================================
// ВАЛІДАЦІЯ ДАНИХ
// =============================================================================

function validateForecastData(data) {
  if (!data.regions_forecast) {
    throw new Error('Missing regions_forecast');
  }

  const THREAT_KEYS = ['ballistic', 'drones', 'cruise', 'guided'];

  for (const [region, hourlyMap] of Object.entries(data.regions_forecast)) {
    if (typeof hourlyMap !== 'object' || Array.isArray(hourlyMap)) {
      throw new Error(`Invalid hourly map for ${region}`);
    }

    for (const [hourKey, slot] of Object.entries(hourlyMap)) {
      if (typeof slot.probability !== 'number' || slot.probability < 0 || slot.probability > 100) {
        throw new Error(`Invalid probability for ${region} hour ${hourKey}`);
      }
      if (typeof slot.alarm !== 'boolean') {
        throw new Error(`Missing alarm boolean for ${region} hour ${hourKey}`);
      }
      // threats: null when alarm=false, object with 4 boolean keys when alarm=true
      if (slot.alarm) {
        if (!slot.threats || typeof slot.threats !== 'object') {
          throw new Error(`Missing threats object for ${region} hour ${hourKey}`);
        }
        for (const key of THREAT_KEYS) {
          if (typeof slot.threats[key] !== 'boolean') {
            throw new Error(`Invalid threats.${key} for ${region} hour ${hourKey}`);
          }
        }
      } else {
        if (slot.threats !== null && slot.threats !== undefined) {
          throw new Error(`threats should be null when alarm=false for ${region} hour ${hourKey}`);
        }
      }
    }
  }

  return true;
}

export { API_BASE_URL, THREAT_LABELS, MODEL_CITIES, calculateStatus, getActiveThreatNames, summarizeHour, fetchForecast, validateForecastData };
