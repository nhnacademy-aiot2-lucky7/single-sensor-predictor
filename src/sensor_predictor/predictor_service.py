from river.linear_model import LinearRegression
from river.preprocessing import StandardScaler
from river.compose import Pipeline
from typing import List, Tuple, Dict, Any


class PredictorService:
    def __init__(self):
        # (sensor_id, sensor_type) => trained model
        self.models: Dict[Tuple[str, str], Pipeline] = {}

    def train_model(self, data: List[Tuple[Dict[str, float], float]]) -> Pipeline:
        """
        주어진 데이터로 모델을 학습함.
        :param data: [(x_dict, y_value), ...] 형식의 데이터
        :return: 학습된 모델 (Pipeline)
        """
        model = Pipeline(StandardScaler(), LinearRegression())

        for x, y in data:
            model.learn_one(x, y)

        return model

    def run_forecast(
            self,
            gateway_id: str,
            sensor_id: str,
            sensor_type: str,
            data: List[Tuple[Dict[str, float], float]],
            state: str
    ) -> Dict[str, Any]:
        """
        센서 데이터를 기반으로 예측을 수행하고 결과 반환
        :param gateway_id: 게이트웨이 ID
        :param sensor_id: 센서 ID
        :param sensor_type: 센서 타입
        :param data: 학습 데이터
        :param state: 상태 (예: completed)
        :return: 예측 결과 (예시 구조)
        """
        if not data:
            return {}

        # 모델 학습
        model = self.train_model(data)

        # 마지막 입력값으로 예측
        last_x, _ = data[-1]
        prediction = model.predict_one(last_x)

        # 모델 저장
        self.models[(sensor_id, sensor_type)] = model

        return {
            "gateway_id": gateway_id,
            "sensor_id": sensor_id,
            "sensor_type": sensor_type,
            "prediction": prediction,
            "state": state
        }

    def get_trained_model(self, sensor_id: str, sensor_type: str) -> Pipeline:
        """
        이전 예측에서 학습된 모델을 반환
        :param sensor_id: 센서 ID
        :param sensor_type: 센서 타입
        :return: Pipeline 객체 또는 None
        """
        return self.models.get((sensor_id, sensor_type))