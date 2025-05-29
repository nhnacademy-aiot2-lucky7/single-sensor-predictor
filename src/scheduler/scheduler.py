import logging
from datetime import datetime
from src.sensor_predictor.influx_service import InfluxService
from src.sensor_predictor.predictor_service import PredictorService
from src.sensor_predictor.sensor_api import load_sensor_list
from src.storage.local_storage import LocalStorage

logging.basicConfig(level=logging.INFO)

class Scheduler:
    def __init__(self, influx: InfluxService, predictor: PredictorService, storage: LocalStorage):
        self.influx = influx
        self.predictor = predictor
        self.storage = storage

    def run(self, predict_range_days: int = 30):
        logging.info("[1/4] ✅ 완료된 센서 리스트 조회 중...")
        sensors = load_sensor_list()
        if not sensors:
            logging.warning("조회된 센서가 없습니다.")
            return

        logging.info(f"[센서 개수] {len(sensors)}개")
        sensor_meta = self.influx.get_sensor_metadata("-90d", sensors)  # 최근 3개월 메타데이터로 제한

        for meta in sensor_meta:
            sensor_id = meta["sensor-id"]
            gateway_id = meta["gateway-id"]
            sensor_type = meta["sensor_type"]

            model, last_trained_time = self.storage.load_model_with_metadata(sensor_id, sensor_type)

            if last_trained_time:
                logging.info(f"[중첩학습] sensor-id={sensor_id}, 마지막 학습시각={last_trained_time}")
                # last_trained_time(datetime)를 Influx 쿼리에 맞는 문자열로 변환 (예: ISO8601)
                train_start = last_trained_time.isoformat() + "Z"
            else:
                logging.info(f"[최초학습] sensor-id={sensor_id}, 전체 데이터 학습")
                train_start = "-90d"  # 기본 3개월치 전체 데이터 학습

            raw_data = self.influx.load_sensor_data(sensor_id, gateway_id, sensor_type, duration=train_start)

            if not raw_data:
                logging.warning(f"⚠️ 학습할 데이터 없음: sensor-id={sensor_id}")
                continue

            trained_model = self.predictor.fit_model(sensor_id, sensor_type, gateway_id, raw_data, model)
            now = datetime.now()
            self.storage.save_model(trained_model, sensor_id, sensor_type, now)

            logging.info(f"🔮 [예측 시작] sensor-id={sensor_id}, 기간={predict_range_days}일")
            forecast = self.predictor.predict(sensor_id, sensor_type, gateway_id, start_time=now, days=predict_range_days)

            if forecast:
                logging.info(f"📤 예측 결과 전송 중 (총 {len(forecast)}건)...")
                self.predictor.send_forecast(sensor_id, forecast)
                logging.info("✅ 예측 전송 완료")
            else:
                logging.warning(f"❌ 예측 실패: sensor-id={sensor_id}")

        logging.info("🎉 모든 센서 예측 완료")
