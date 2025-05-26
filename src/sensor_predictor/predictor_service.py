from river.compose import Pipeline
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime, timedelta
from river.linear_model import LinearRegression
from river.preprocessing import StandardScaler


class PredictorService:
    def __init__(self):
        self.models: Dict[Tuple[str, str], Pipeline] = {}

    def run_forecast(
            self,
            gateway_id: str,
            sensor_id: str,
            sensor_type: str,
            train_data: List[Tuple[Dict[str, float], float]]
    ) -> Optional[Dict[str, Any]]:
        if not train_data:
            return None

        model = Pipeline(StandardScaler(), LinearRegression())

        # 학습
        for x, y in train_data:
            model.learn_one(x, y)

        self.models[(sensor_id, sensor_type)] = model

        # 마지막 학습 시각 기준으로 다음 5일(120시간) 예측
        last_record_time = max(
            datetime.strptime(f"{int(x['day'])} {int(x['hour'])}", "%Y %H")
            for x, _ in train_data
        )
        prediction_start = last_record_time + timedelta(hours=1)

        predicted_values = []
        for i in range(5 * 24):  # 5일 * 24시간 = 120개
            current_time = prediction_start + timedelta(hours=i)
            x = {
                "day": current_time.timetuple().tm_yday,
                "hour": current_time.hour
            }
            y_pred = model.predict_one(x)
            predicted_values.append({
                "predictedValue": round(y_pred, 2),
                "predictedDate": current_time.strftime("%Y-%m-%dT%H:%M:%S")
            })

        analyzed_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        return {
            "analysisType": "SINGLE_SENSOR_PREDICT",
            "result": {
                "sensorInfo": {
                    "gatewayId": gateway_id,
                    "sensorId": sensor_id,
                    "sensorType": sensor_type
                },
                "model": "river",
                "predictedData": predicted_values,
                "analyzedAt": analyzed_at
            }
        }

    def get_trained_model(self, sensor_id: str, sensor_type: str) -> Optional[Pipeline]:
        return self.models.get((sensor_id, sensor_type))
