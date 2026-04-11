# AWS Deployment Guide для WarEvents

Інструкція з розгортування батч-предикції та REST API на AWS EC2.

---

## 📋 Структура на AWS

```
/home/ec2-user/WarEvents/
├── src/
│   ├── forecast_pipeline/
│   │   ├── batch_predict.py          ← Запускається cron кожну годину
│   │   ├── 3__hist_gradient_boosting__v1.pkl
│   │   ├── isw_*.pkl
│   │   └── ... (інші скрипти)
│   ├── saas/
│   │   ├── app.py                    ← Flask API (постійно запущений)
│   │   └── run_api.sh
│   └── data_collection/
├── data/
│   ├── final/FINAL_FEATURES_24H.csv
│   ├── predictions/predictions_latest.json  ← Виходить batch_predict.py
│   └── logs/
│       ├── batch_predict.log
│       └── api.log
├── requirements.txt
└── crontab.txt
```

---

## 🚀 Кроки розгортування

### 1️⃣ SSH на сервер і клонування проекту

```bash
ssh -i your-key.pem ec2-user@your-aws-ip

# Клонування репо
cd /home/ec2-user
git clone https://github.com/YOUR-REPO/WarEvents.git
cd WarEvents

# Перевірка структури
ls -la src/forecast_pipeline/ | grep pkl
ls -la src/saas/
```

### 2️⃣ Налаштування Python环境

```bash
# Оновлення системи
sudo yum update -y
sudo yum install python3.11 python3.11-pip git -y

# Створення virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Установка залежностей
pip install --upgrade pip
pip install -r requirements.txt

# Додатково для API:
pip install flask flask-cors gunicorn
```

### 3️⃣ Перевірка моделей на сервері

```bash
# Моделі мають бути завантажені з Google Drive або git-lfs
ls -lh src/forecast_pipeline/*.pkl

# Очікувані файли:
# - 3__hist_gradient_boosting__v1.pkl (binary model)
# - isw_tfidf_vectorizer.pkl (ISW feature engineering)
# - isw_kw_dict.pkl (ISW keywords)

# Якщо моделей нема — завантажити з Google Drive:
# (вручну або через скрипт)
```

### 4️⃣ Тестування batch prediction локально

```bash
source venv/bin/activate

# Перевірка що FINAL_FEATURES_24H.csv існує
python3 -c "import pandas as pd; df = pd.read_csv('data/final/FINAL_FEATURES_24H.csv'); print(f'Shape: {df.shape}')"

# Запуск batch_predict.py вручну
python src/forecast_pipeline/batch_predict.py

# Перевірка виходу
cat data/predictions/predictions_latest.json | jq .
```

### 5️⃣ Налаштування Cron для батч-предикції

```bash
# Редагування crontab
crontab -e

# Додати цей рядок (запускається кожну годину о :08 хвилині):
# Приклад: 08:00, 09:00, 10:00 тощо
8 * * * * cd /home/ec2-user/WarEvents && source venv/bin/activate && python src/forecast_pipeline/batch_predict.py >> /home/ec2-user/WarEvents/data/logs/cron.log 2>&1

# Перевірка crontab
crontab -l

# Перевірка логів cron
tail -f data/logs/cron.log
```

**⚠️ ВАЖЛИВО:** 
- Час виконання має бути **до** часу запуску API запиту (якщо клієнт запитує о :10, cron має запуститися о :08)
- Timezone — UTC (перевіри через `date -u`)

### 6️⃣ Запуск Flask API

#### Варіант A: На одиночному порту (development)

```bash
source venv/bin/activate
python src/saas/app.py --host 0.0.0.0 --port 5000
```

#### Варіант B: Через Gunicorn (production) з systemd

Створити файл `/etc/systemd/system/warevents-api.service`:

```ini
[Unit]
Description=WarEvents Alarm Forecast API
After=network.target

[Service]
Type=notify
User=ec2-user
WorkingDirectory=/home/ec2-user/WarEvents
Environment="PATH=/home/ec2-user/WarEvents/venv/bin"
ExecStart=/home/ec2-user/WarEvents/venv/bin/gunicorn \
    --workers 4 \
    --worker-class sync \
    --bind 0.0.0.0:5000 \
    --timeout 30 \
    --access-logfile /home/ec2-user/WarEvents/data/logs/gunicorn_access.log \
    --error-logfile /home/ec2-user/WarEvents/data/logs/gunicorn_error.log \
    src.saas.app:app

Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Запуск:

```bash
sudo systemctl daemon-reload
sudo systemctl start warevents-api
sudo systemctl enable warevents-api  # Auto-start on boot

# Перевірка статусу
sudo systemctl status warevents-api
sudo journalctl -u warevents-api -f
```

### 7️⃣ Налаштування AWS Security Group

В AWS Console → EC2 → Security Groups:

| Protocol | Port | Source | Purpose |
|----------|------|--------|---------|
| TCP | 5000 | 0.0.0.0/0 | Flask API (HTTP) |
| TCP | 443 | 0.0.0.0/0 | HTTPS (later with nginx) |
| TCP | 22 | YOUR_IP/32 | SSH (вас IP) |

### 8️⃣ SSL/HTTPS через Nginx (production)

```bash
# Установка Nginx
sudo yum install nginx -y
sudo systemctl start nginx
sudo systemctl enable nginx

# Установка Certbot
sudo yum install certbot python3-certbot-nginx -y

# Отримання SSL сертифіката
sudo certbot certonly --standalone -d your-domain.com

# Налаштування Nginx як reverse proxy
sudo vi /etc/nginx/conf.d/warevents.conf
```

```nginx
upstream warevents_api {
    server 127.0.0.1:5000;
    keepalive 32;
}

server {
    listen 80;
    server_name your-domain.com;

    root /home/ubuntu/war-events-predicting/alarm_pred/dist;
    index index.html;

    location /api/ {
        proxy_pass http://warevents_api;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location = /health {
        proxy_pass http://warevents_api;
        proxy_set_header Host $host;
    }

    location / {
        try_files $uri $uri/ /index.html;
    }
}
```

Готовий шаблон конфігу є в репозиторії:

- `deploy/nginx/warevents.conf`

Кроки для застосування:

```bash
cd /home/ubuntu/war-events-predicting/alarm_pred
npm install
npm run build

sudo cp /home/ubuntu/war-events-predicting/deploy/nginx/warevents.conf /etc/nginx/conf.d/warevents.conf
sudo nginx -t
sudo systemctl reload nginx
```

Після переходу на Nginx можна закрити публічний доступ до 5000 порту в Security Group
і залишити Flask/Gunicorn доступним лише локально (127.0.0.1).

Перезавантажити Nginx:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

### 9️⃣ Моніторинг та логування

```bash
# Перевірка cron логів
tail -100f data/logs/cron.log

# Перевірка batch_predict логів
tail -100f data/logs/batch_predict.log

# Перевірка API логів
tail -100f data/logs/api.log

# Перевірка Gunicorn (якщо використовується)
sudo journalctl -u warevents-api -f

# Перевірка диск-спейсу
df -h

# Перевірка дозволів файлів
ls -la data/predictions/
ls -la data/logs/
```

---

## 🧪 Тестування API

```bash
# Local testing (before deployment)
python src/saas/app.py --debug --host 127.0.0.1 --port 5000

# In another terminal:
curl "http://localhost:5000/api/forecast?region=Kyiv"
curl "http://localhost:5000/api/forecast?region=all"
curl "http://localhost:5000/health"
```

```bash
# Remote testing (AWS)
curl "http://your-aws-ip:5000/api/forecast?region=Kyiv"

# With SSL (Nginx)
curl "https://your-domain.com/api/forecast?region=Kyiv"
```

---

## 📊 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/api/forecast?region=Kyiv` | Get forecast for region |
| GET | `/api/forecast?region=all` | Get forecast for all regions |
| GET | `/api/regions` | List available regions |
| GET | `/api/threat-types` | Get threat type definitions |
| GET | `/api/metadata` | Get API metadata |

---

## 🔧 Troubleshooting

### Cron не запускається

```bash
# Перевірка синтаксису cron
sudo crontab -l -u ec2-user

# Перевірка логів системи
sudo tail -50f /var/log/cron

# Тестування команди вручну
cd /home/ec2-user/WarEvents && source venv/bin/activate && python src/forecast_pipeline/batch_predict.py
```

### API повертає "No predictions available"

```bash
# Перевірка що predictions_latest.json існує
ls -la data/predictions/predictions_latest.json

# Якщо не існує — запустити batch_predict вручну
python src/forecast_pipeline/batch_predict.py

# Перевірка файлу
cat data/predictions/predictions_latest.json | jq .
```

### ModuleNotFoundError при імпорту

```bash
# Перевірка venv активізований
which python  # має показати path до /venv/bin/python

# Переустановка залежностей
source venv/bin/activate
pip install -r requirements.txt
```

### Port 5000 вже займатий

```bash
# Знайти процес що використовує port
lsof -i :5000

# Kill процес (якщо потрібно)
kill -9 PID_NUMBER
```

---

## 📝 Checklist для production

- [ ] Моделі завантажені на AWS (pkl файли)
- [ ] Virtual environment налаштований
- [ ] Залежності установлені (`pip install -r requirements.txt`)
- [ ] Batch prediction протестований локально
- [ ] Cron налаштований і запущений
- [ ] API протестований локально
- [ ] API запущений на сервері (gunicorn або systemd)
- [ ] Security Group відкритий для портів (5000, 22, 443)
- [ ] SSL сертифікат установлений (Nginx reverse proxy)
- [ ] Логування налаштовано і перевірено
- [ ] Моніторинг установлений (CloudWatch, або просто tail логів)
- [ ] Фронтенд вказує на правильний API URL

---

## 🔐 Security Best Practices

1. **API Key** (опціонально):
   ```python
   # Додати в app.py
   @app.before_request
   def check_api_key():
       if request.path.startswith('/api/'):
           key = request.headers.get('X-API-Key')
           if key != os.getenv('API_KEY'):
               return jsonify({"error": "Unauthorized"}), 401
   ```

2. **Rate Limiting**:
   ```bash
   pip install flask-limiter
   ```

3. **HTTPSOnly** — Завжди використовуйте SSL в production

4. **Firewall Rules** — Обмежте доступ до 22 дивно тільки вашому IP

5. **Регулярні бекапи** моделей и даних на S3

---

## 📞 Support

Якщо виникають проблеми:

1. Перевірте логи: `data/logs/batch_predict.log`, `data/logs/api.log`
2. Запустіть批 скрипт вручну та подивіться виходу
3. Перевірте дозволи файлів: `chmod 755 src/forecast_pipeline/`
4. Перевірте timezone на сервері: `date -u`

