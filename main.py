import logging
import os
import time
import schedule

from dotenv import load_dotenv
from src.config import logging_setup
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

def job():
    logging.info("🕑 새벽 2시 스케줄러 실행 중...")
    influx = InfluxService(INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET)
    predictor = PredictorService()
    storage = LocalStorage()
    scheduler = Scheduler(influx, predictor, storage)
    scheduler.run(predict_range_days=30)

if __name__ == "__main__":
    # ✅ 최초 1회 실행
    logging.info("🚀 최초 1회 실행 중...")
    job()

    # ✅ 매일 새벽 2시에 job() 실행
    schedule.every().day.at("02:00").do(job)

    logging.info("⏳ 스케줄러 대기 중... (매일 새벽 2시 실행)")
    while True:
        schedule.run_pending()
        time.sleep(60)