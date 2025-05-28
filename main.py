import logging
import os
import json
from datetime import datetime

from src.sensor_predictor.influx_service import InfluxService
from src.sensor_predictor.predictor_service import PredictorService
from src.sensor_predictor.sensor_api import load_sensor_list
from src.storage.local_storage import LocalStorage
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)

load_dotenv()

INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
DURATION = os.getenv("DURATION", "-7d")
PREDICT_DAYS = int(os.getenv("PREDICT_DAYS", 1))

# 서비스 초기화
influx = InfluxService(INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET)
predictor = PredictorService()
storage = LocalStorage()

SENSOR_ID = "sensor-id"
GATEWAY_ID = "gateway-id"
SENSOR_TYPE = "sensor_type"

def run_prediction_service(predict_days: int):
    logging.info("📡 [1/3] completed 센서 목록 조회 중...")

    completed_sensors = load_sensor_list()

    logging.info(f"[센서 개수] {len(completed_sensors)}개")
    logging.info("📡 [2/3] 센서 메타데이터 조회 중...")

    sensor_meta = influx.get_sensor_metadata(DURATION, completed_sensors)

    logging.info(f"센서 메타데이터 개수: {len(sensor_meta)}")
    if not sensor_meta:
        logging.warning("센서 메타데이터가 비어있습니다! 예측 불가.")
        return

    logging.info("🔮 [3/3] 센서 예측 시작...")
    for sensor in sensor_meta:
        raw_data = influx.load_sensor_data(
            sensor[SENSOR_ID],
            sensor[GATEWAY_ID],
            sensor[SENSOR_TYPE],
            DURATION
        )

        if not raw_data:
            logging.warning(f"⚠️ 학습 데이터 없음 - sensor-id={sensor[SENSOR_ID]}, gateway_id={sensor[GATEWAY_ID]}")
            continue

        logging.info("여기까지 출력 !!!")

        result = predictor.run_forecast(
            sensor[GATEWAY_ID],
            sensor[SENSOR_ID],
            sensor[SENSOR_TYPE],
            raw_data,
            predict_days=predict_days
        )

        if result:
            logging.info("📈 예측 결과:")
            print(json.dumps(result, indent=2))

            model = predictor.get_trained_model(
                sensor[GATEWAY_ID],
                sensor[SENSOR_ID],
                sensor[SENSOR_TYPE],
            )
            if model:
                base_date = datetime.now()
                storage.save_model(model, sensor[SENSOR_ID], sensor[SENSOR_TYPE], base_date)
                logging.info(f"💾 모델 저장 완료: sensor-id={sensor[SENSOR_ID]} 날짜={base_date.isoformat()}")
        else:
            logging.warning(f"❌ 예측 결과 없음: sensor-id={sensor[SENSOR_ID]}")

    logging.info("✅ 예측 서비스 완료")

def run_job(predict_days: int):
    try:
        logging.info("예측 서비스 시작")
        run_prediction_service(predict_days)
        logging.info("예측 서비스 완료")
    except Exception as e:
        logging.error(f"예측 서비스 에러 발생: {e}", exc_info=True)

if __name__ == "__main__":
    default_predict_days = int(os.getenv("PREDICT_DAYS", PREDICT_DAYS))
    run_job(default_predict_days)
