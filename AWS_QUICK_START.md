# Quick Start для AWS EC2 (15 хвилин)

## 0️⃣ Припущення

- AWS EC2 інстанс з Amazon Linux 2
- Git SSH ключ налаштований
- Domain + SSL сертифікат (опціонально)

## 1️⃣ SSH на сервер (2 хв)

```bash
ssh -i your-key.pem ec2-user@YOUR-AWS-IP
```

## 2️⃣ Клонування проекту (2 хв)

```bash
cd /home/ec2-user
git clone https://github.com/YOUR/WarEvents.git
cd WarEvents

# Перевірка структури
ls src/saas/
ls src/forecast_pipeline/
```

## 3️⃣ Встановлення залежностей (4 хв)

```bash
# System packages
sudo yum update -y
sudo yum install python3.11 python3.11-pip git -y

# Python virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Python packages
pip install --upgrade pip
pip install -r requirements.txt
```

## 4️⃣ Завантаження моделей (2 хв)

Залежить від того де у вас моделі:

### Опція А: Git LFS
```bash
git lfs install
git lfs pull
```

### Опція Б: Google Drive
```bash
# Завантажити models/ папку з Google Drive
# Скопіювати в /home/ec2-user/WarEvents/src/forecast_pipeline/

ls src/forecast_pipeline/*.pkl
```

### Опція В: S3
```bash
aws s3 cp s3://your-bucket/models/ src/forecast_pipeline/ --recursive
```

## 5️⃣ Налаштування Cron (1 хв)

```bash
# Редагування crontab
crontab -e

# Добавити цей рядок:
8 * * * * cd /home/ec2-user/WarEvents && source venv/bin/activate && python src/forecast_pipeline/batch_predict.py >> data/logs/cron.log 2>&1

# Перевірка
crontab -l
```

## 6️⃣ Запуск API (2 хв)

### Варіант 1: Direct (development)
```bash
source venv/bin/activate
python src/saas/app.py --host 0.0.0.0 --port 5000
```

### Варіант 2: Gunicorn + Systemd (production)

Створити `/etc/systemd/system/warevents-api.service`:

```bash
sudo bash -c 'cat > /etc/systemd/system/warevents-api.service << "EOF"
[Unit]
Description=WarEvents Forecast API
After=network.target

[Service]
User=ec2-user
WorkingDirectory=/home/ec2-user/WarEvents
ExecStart=/home/ec2-user/WarEvents/venv/bin/gunicorn \
    --workers 4 --bind 0.0.0.0:5000 \
    --access-logfile data/logs/gunicorn_access.log \
    --error-logfile data/logs/gunicorn_error.log \
    src.saas.app:app
Restart=always

[Install]
WantedBy=multi-user.target
EOF'
```

Запуск:

```bash
sudo systemctl daemon-reload
sudo systemctl start warevents-api
sudo systemctl enable warevents-api
sudo systemctl status warevents-api
```

## 7️⃣ Тестування (1 хв)

```bash
# Перевірка API
curl "http://localhost:5000/health"
curl "http://localhost:5000/api/forecast?region=Kyiv"

# Перевірка логів
tail -20 data/logs/api.log
tail -20 data/logs/batch_predict.log
tail -20 data/logs/cron.log
```

## 🎯 Результат

- API запущено на `http://YOUR-AWS-IP:5000/api/forecast`
- Batch predictions генеруються кожну годину
- Логи: `data/logs/`

## 📋 Checklist

- [ ] Python 3.11 встановлено
- [ ] Залежності встановлені
- [ ] Моделі завантажені (`ls src/forecast_pipeline/*.pkl`)
- [ ] FINAL_FEATURES_24H.csv існує
- [ ] Cron налаштований (`crontab -l`)
- [ ] API запущено і респондує на `/health`
- [ ] Фронтенд вказує на правильний URL

## 🆘 Проблеми?

```bash
# Перевірка Python
python --version

# Перевірка venv
which python

# Перевірка моделей
ls -lh src/forecast_pipeline/*.pkl

# Перевірка features
python -c "import pandas as pd; print(pd.read_csv('data/final/FINAL_FEATURES_24H.csv').shape)"

# Запуск batch скрипту вручну
python src/forecast_pipeline/batch_predict.py

# Перевірка API
curl http://localhost:5000/health
```

Готово! 🚀
