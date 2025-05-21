import logging
import os
from datetime import datetime

from src.sensor_predictor.influx_service import InfluxService
from src.sensor_predictor.predictor_service import PredictorService
from src.sensor_predictor.sensor_api import get_sensor_list_by_state
from src.storage.local_storage import LocalStorage
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)

load_dotenv()

INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
DURATION = os.getenv("DURATION", "-5d")

# 서비스 초기화
influx = InfluxService(INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET)
predictor = PredictorService()
storage = LocalStorage()

def run_prediction_service():
    logging.info("📡 [1/3] completed 센서 목록 조회 중...")

    completed_sensors = get_sensor_list_by_state("completed")
    if not completed_sensors:
        logging.warning("예측할 completed 센서가 없습니다.")
        return

    logging.info("📡 [2/3] 센서 메타데이터 조회 중...")
    sensor_meta = influx.get_sensor_metadata(DURATION, completed_sensors)

    logging.info("🔮 [3/3] 센서 예측 시작...")
    for sensor in sensor_meta:
        data = influx.load_sensor_data(sensor["sensor_id"], sensor["gateway_id"], sensor["sensor_type"], DURATION)
        result = predictor.run_forecast(sensor["gateway_id"], sensor["sensor_id"], sensor["sensor_type"], data, "completed")

        if result:
            # 1. 결과 출력 (필요시)
            logging.info(f"예측 완료: {result}")

            # 2. 모델 저장 (river 모델을 예로 들어 predictor 내부에 모델이 있다고 가정)
            # 실제로는 predictor에서 학습된 모델 객체를 받아와야 함
            model = predictor.get_trained_model(sensor["sensor_id"], sensor["sensor_type"])  # 임의 함수
            if model:
                base_date = datetime.now()  # 또는 예측 데이터 기준 날짜
                storage.save_model(model, sensor["sensor_id"], base_date)
                logging.info(f"모델 저장 완료: sensor_id={sensor['sensor_id']} 날짜={base_date.strftime('%Y-%m-%d')}")

    logging.info("✅ 예측 서비스 완료")

def run_job():
    try:
        logging.info("예측 서비스 시작")
        run_prediction_service()
        logging.info("예측 서비스 완료")
    except Exception as e:
        logging.error(f"예측 서비스 에러 발생: {e}", exc_info=True)
