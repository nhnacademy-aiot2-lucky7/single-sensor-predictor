from datetime import datetime, timedelta
from river import linear_model, preprocessing

class PredictorService:
    def __init__(self):
        # 모델 파이프라인 초기화 (StandardScaler + LinearRegression)
        self.model = preprocessing.StandardScaler() | linear_model.LinearRegression()

    def train(self, raw_train_data):
        """
        raw_train_data: list of dict
          각 원소는 {"features": dict, "target": float} 형태
        """
        for data_point in raw_train_data:
            features = data_point["features"]
            target = data_point["target"]
            self.model.learn_one(features, target)
        return self.model

    def predict(self, model, start_time: datetime, end_time: datetime, interval_hours=1):
        """
        start_time부터 end_time까지 interval_hours 단위로 예측 수행
        """
        current_time = start_time
        predictions = []
        while current_time < end_time:
            features = self.make_features(current_time)
            predicted_value = model.predict_one(features)
            predictions.append({"timestamp": current_time, "value": predicted_value})
            current_time += timedelta(hours=interval_hours)
        return predictions

    def make_features(self, dt: datetime):
        """
        예측 시 필요한 feature 생성
        """
        return {
            "hour": dt.hour,
            "weekday": dt.weekday()
        }

    def run_forecast(self, gateway_id, sensor_id, sensor_type, raw_train_data, predict_days: int):
        """
        전체 학습과 예측 수행
        - raw_train_data: 학습 데이터 리스트 [{"features":..., "target":...}, ...]
        - predict_days: 예측할 일 수 (예: 1이면 24시간 예측)

        동작:
        1. 학습 데이터로 모델 학습
        2. 학습 완료 시점을 현재 시각으로 잡고,
        3. 학습 완료 시점 다음 시간부터 predict_days * 24시간 만큼 1시간 단위 예측 수행
        4. 예측 결과 리스트 반환
        """

        # 1) 모델 학습
        model = self.train(raw_train_data)

        # 2) 학습 완료 시점
        train_complete_time = datetime.now()

        # 3) 예측 시작 시점 (학습 완료 시점 바로 다음 시간부터)
        predict_start_time = train_complete_time + timedelta(hours=1)

        # 4) 예측 종료 시점
        predict_end_time = predict_start_time + timedelta(days=predict_days)

        # 5) 예측 실행
        predictions = self.predict(model, predict_start_time, predict_end_time, interval_hours=1)

        # 6) 예측 결과 리턴
        return {
            "gateway_id": gateway_id,
            "sensor_id": sensor_id,
            "sensor_type": sensor_type,
            "trained_at": train_complete_time.isoformat(),
            "predictions": predictions
        }