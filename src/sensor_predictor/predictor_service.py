import logging
import os
import pytz
from datetime import datetime, timedelta

import requests
from river import linear_model, preprocessing, metrics
from src.storage.local_storage import LocalStorage

logger = logging.getLogger(__name__)

class PredictorService:
    def __init__(self):
        self.models = {}
        self.metric = metrics.MAE()
        self.storage = LocalStorage()
        self.last_features = {}

    def fit_model(self, sensor_id, sensor_type, gateway_id, data, model=None):
        key = (gateway_id, sensor_id, sensor_type)
        if model is None:
            model = preprocessing.StandardScaler() | linear_model.LinearRegression()

        # 기존 모델 성능 평가
        previous_model = self.models.get(key)
        prev_metric = None
        if previous_model:
            prev_metric = self.evaluate_model(previous_model, data)
            logging.info(f"기존 모델 성능 (MAE): {prev_metric['MAE']}")

        # 모델 학습
        for record in data:
            x = record["features"]
            y = record["target"]
            model.learn_one(x, y)

        # 새로운 모델 성능 평가
        new_metric = self.evaluate_model(model, data)
        logging.info(f"새로운 모델 성능 (MAE): {new_metric['MAE']}")

        # 성능이 개선된 경우에만 모델을 저장
        if not previous_model or new_metric["MAE"] < prev_metric["MAE"] * 0.9:
            self.models[key] = model
            if data:
                self.last_features[key] = data[-1]["features"]
            logging.info(f"모델이 업데이트되었습니다: sensor-id={sensor_id}")
        else:
            logging.info(f"모델 성능이 개선되지 않았습니다. 업데이트하지 않음: sensor-id={sensor_id}")

        return model

    def evaluate_model(self, model, data):
        """모델 성능 평가 (MAE 기준)"""
        mae_metric = metrics.MAE()
        rmse_metric = metrics.RMSE()
        for record in data:
            x = record["features"]
            y = record["target"]
            y_pred = model.predict_one(x)
            mae_metric.update(y, y_pred)
            rmse_metric.update(y, y_pred)
        return {"MAE": mae_metric.get(), "RMSE": rmse_metric.get()}

    def predict(self, sensor_id, sensor_type, gateway_id, start_time: datetime, days: int = 30):
        key = (gateway_id, sensor_id, sensor_type)
        model = self.models.get(key)
        if model is None:
            logging.warning(f"❌ 예측 모델이 없습니다: sensor-id={sensor_id}")
            return None

        last_feature = self.last_features.get(key)
        if last_feature is None:
            logging.warning(f"❌ 예측에 사용할 feature가 없습니다: sensor-id={sensor_id}")
            return None

        predicted_data = []
        KST = pytz.timezone('Asia/Seoul')

        # start_time이 naive datetime이면 KST 적용
        if start_time.tzinfo is None:
            start_time = KST.localize(start_time)

        current_time = start_time
        current_feature = dict(last_feature)  # 복사해서 예측 반복에 사용

        for i in range(days * 24):  # 1시간 단위 예측
            try:
                predicted_value = model.predict_one(current_feature)
            except Exception as e:
                logging.error(f"예측 오류 발생: {e}")
                predicted_value = None

            # 다음 입력값에 predicted_value를 target으로 사용
            current_feature["target"] = predicted_value

            predicted_time = int((current_time + timedelta(hours=i)).timestamp() * 1000)
            predicted_data.append({
                "predictedValue": predicted_value,
                "predictedDate": predicted_time
            })

        result = {
            "result": {
                "type": "SINGLE_SENSOR_PREDICT",
                "sensorInfo": {
                    "gatewayId": gateway_id,
                    "sensorId": sensor_id,
                    "sensorType": sensor_type
                },
                "model": "river",
                "predictedData": predicted_data,
                "analyzedAt": int(datetime.now().timestamp() * 1000)
            }
        }

        return result

    def send_forecast(self, sensor_id, forecast_result):
        url = os.getenv("API_URL")
        headers = {"Content-Type": "application/json"}

        try:
            response = requests.post(url, json=forecast_result, headers=headers)
            response.raise_for_status()
            logging.info(f"✅ Forecast sent successfully for sensor '{sensor_id}'")
        except requests.RequestException as e:
            logging.error(f"❌ Forecast 전송 실패 for sensor '{sensor_id}': {e}")