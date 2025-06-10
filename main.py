import logging
import os
import time
from datetime import datetime

import schedule

from dotenv import load_dotenv

from src.config import logging_setup
logging_setup.setup_logging()

from src.sensor_predictor.influx_service import InfluxService
from src.sensor_predictor.predictor_service import PredictorService
from src.storage.local_storage import LocalStorage
from src.scheduler.scheduler import Scheduler

load_dotenv()

INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

logger = logging.getLogger(__name__)
logger.info("🔥 시스템 시작")

def job(start_dates=None):
    logging.info("🕑 새벽 2시 스케줄러 실행 중...")
    influx = InfluxService(INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET)
    predictor = PredictorService()
    storage = LocalStorage()
    scheduler = Scheduler(influx, predictor, storage)

    scheduler.run(predict_range_days=30, start_dates=start_dates)

if __name__ == "__main__":
    # ✅ 최초 1회 실행
    logging.info("🚀 최초 1회 실행 중...")

    manual_dates = [
        datetime(2025, 6, 8),
        datetime(2025, 6, 9),
        datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    ]
    job(start_dates=manual_dates)

    def daily_job():
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        job(start_dates=[today])

    # ✅ 매일 새벽 2시에 job() 실행
    schedule.every().day.at("02:00").do(daily_job)

    logging.info("⏳ 스케줄러 대기 중... (매일 새벽 2시 실행)")
    while True:
        schedule.run_pending()
        time.sleep(60)