import logging
from datetime import datetime
import json

from src.sensor_predictor.influx_service import InfluxService
from src.sensor_predictor.predictor_service import PredictorService
from src.sensor_predictor.sensor_api import load_sensor_list
from src.storage.local_storage import LocalStorage

logger = logging.getLogger(__name__)

class Scheduler:
    def __init__(self, influx: InfluxService, predictor: PredictorService, storage: LocalStorage):
        self.influx = influx
        self.predictor = predictor
        self.storage = storage

    def run(self, predict_range_days: int = 30, start_dates: list[datetime] = None):
        logging.info("[1/3] 완료된 센서 리스트 조회 중...")

        sensors = load_sensor_list()
        if not sensors:
            logging.warning("조회된 센서가 없습니다.")
            return

        logging.info(f"[센서 개수] {len(sensors)}개")
        sensor_meta = self.influx.get_sensor_metadata("-90d", sensors)  # 최근 3개월 메타데이터로 제한

        if start_dates is None:
            start_dates = []

        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        if today not in start_dates:
            start_dates.append(today)

        for date in start_dates:
            logging.info(f"📅 예측 기준 날짜: {date.strftime('%Y-%m-%d')}")

            for meta in sensor_meta:
                sensor_id = meta["sensor_id"]
                gateway_id = meta["gateway_id"]
                sensor_type = meta["sensor_type"]

                model, last_trained_time = self.storage.load_model_with_metadata(sensor_id, sensor_type)

                if last_trained_time:
                    logging.info(f"[2/3] [중첩학습] sensor-id={sensor_id}, 마지막 학습시각={last_trained_time}")
                    # last_trained_time(datetime)를 Influx 쿼리에 맞는 문자열로 변환 (예: ISO8601)
                    train_start = last_trained_time.isoformat() + "Z"
                else:
                    logging.info(f"[2/3] [최초학습] sensor-id={sensor_id}, 전체 데이터 학습")
                    train_start = "-90d"  # 기본 3개월치 전체 데이터 학습

                raw_data = self.influx.load_sensor_data(sensor_id, gateway_id, sensor_type, duration=train_start)

                if not raw_data:
                    logging.warning(f"⚠️ 학습할 데이터 없음: sensor-id={sensor_id}")
                    continue

                # 실제값의 최대, 최소 구하기
                actual_values = [record["target"] for record in raw_data if record["target"] is not None]

                if not actual_values:
                    logger.warning("actual_values가 비어 있습니다.")
                    continue

                min_value = min(actual_values)
                max_value = max(actual_values)

                trained_model = self.predictor.fit_model(sensor_id, sensor_type, gateway_id, raw_data, model)
                self.storage.save_model(trained_model, sensor_id, sensor_type, date)

                logging.info(f"[3/3] [예측 시작] sensor-id={sensor_id}, 기간={predict_range_days}일")
                forecast = self.predictor.predict(sensor_id, sensor_type, gateway_id,
                                                  min_value,
                                                  max_value,
                                                  start_time=date,
                                                  days=predict_range_days)

                logging.info(f"forecast 개수: {len(forecast)}")

                if forecast:
                    logging.info(f"예측 결과 전송 중 (총 {len(forecast)}건)...")
                    logging.info(json.dumps(forecast, indent=2, ensure_ascii=False))  # 한글 시간 포맷 유지
                    self.predictor.send_forecast(sensor_id, forecast)
                    logging.info(f"✅ 예측 전송 완료 (총 {len(forecast)}건)...")
                else:
                    logging.warning(f"❌ 예측 실패: sensor-id={sensor_id}")

        logging.info("🎉 모든 센서 예측 완료")