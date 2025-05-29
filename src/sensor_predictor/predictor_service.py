import logging
from datetime import datetime, timedelta
from river import linear_model, preprocessing, metrics
from src.storage.local_storage import LocalStorage

class PredictorService:
    def __init__(self):
        self.models = {}
        self.metric = metrics.MAE()
        self.storage = LocalStorage()

    def fit_model(self, sensor_id, sensor_type, gateway_id, data, model=None):
        key = (gateway_id, sensor_id, sensor_type)
        if model is None:
            model = preprocessing.StandardScaler() | linear_model.LinearRegression()
        for record in data:
            x = record["features"]
            y = record["target"]
            model.learn_one(x, y)
        self.models[key] = model
        return model

    def predict(self, sensor_id, sensor_type, gateway_id, start_time: datetime, days: int = 30):
        key = (gateway_id, sensor_id, sensor_type)
        model = self.models.get(key)
        if not model:
            logging.warning(f"모델 없음: sensor-id={sensor_id}")
            return None

        forecast = []
        current_time = start_time
        end_time = start_time + timedelta(days=days)
        while current_time < end_time:
            features = {
                "hour": current_time.hour,
                "day": current_time.day,
                "weekday": current_time.weekday()
            }
            y_pred = model.predict_one(features)
            forecast.append({
                "timestamp": current_time.isoformat(),
                "prediction": y_pred
            })
            current_time += timedelta(hours=1)
        return forecast

    def get_trained_model(self, gateway_id, sensor_id, sensor_type):
        return self.models.get((gateway_id, sensor_id, sensor_type))

    def send_forecast(self, sensor_id, forecast_data):
        # 실제 API 전송 로직으로 대체 필요
        logging.info(f"📡 [전송됨] sensor={sensor_id}, 예측 수={len(forecast_data)}")