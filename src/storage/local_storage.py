import os
import pickle
from typing import Optional
from river.compose import Pipeline

class LocalStorage:
    def __init__(self, base_dir="./models"):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    def save_model(self, model: Pipeline, sensor_id: str, base_date) -> None:
        save_path = os.path.join(self.base_dir, base_date.strftime("%Y-%m-%d"))
        os.makedirs(save_path, exist_ok=True)
        filename = f"model_{sensor_id}_{base_date.strftime('%Y%m%d')}.pkl"
        filepath = os.path.join(save_path, filename)
        try:
            with open(filepath, "wb") as f:
                pickle.dump(model, f)
        except Exception as e:
            # 필요에 따라 로깅 추가 가능
            raise RuntimeError(f"모델 저장 실패: {e}")

    def load_model(self, sensor_id: str, base_date) -> Optional[Pipeline]:
        load_path = os.path.join(self.base_dir, base_date.strftime("%Y-%m-%d"))
        filename = f"model_{sensor_id}_{base_date.strftime('%Y%m%d')}.pkl"
        filepath = os.path.join(load_path, filename)
        if os.path.exists(filepath):
            try:
                with open(filepath, "rb") as f:
                    return pickle.load(f)
            except Exception as e:
                # 필요에 따라 로깅 추가 가능
                raise RuntimeError(f"모델 로드 실패: {e}")
        return None