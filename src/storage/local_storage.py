import os
import pickle
import json
from datetime import datetime

class LocalStorage:
    def __init__(self, base_dir="saved_models"):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    def _model_path(self, sensor_id, sensor_type):
        return os.path.join(self.base_dir, f"{sensor_id}_{sensor_type}_model.pkl")

    def _metadata_path(self, sensor_id, sensor_type):
        return os.path.join(self.base_dir, f"{sensor_id}_{sensor_type}_meta.json")

    def save_model(self, model, sensor_id, sensor_type, trained_time: datetime):
        # 모델 저장
        model_path = self._model_path(sensor_id, sensor_type)
        with open(model_path, "wb") as f:
            pickle.dump(model, f)

        # 학습 시각 메타데이터 저장
        meta_path = self._metadata_path(sensor_id, sensor_type)
        with open(meta_path, "w") as f:
            json.dump({"last_trained_time": trained_time.isoformat()}, f)

    def load_model_with_metadata(self, sensor_id, sensor_type):
        model = None
        last_trained_time = None

        model_path = self._model_path(sensor_id, sensor_type)
        meta_path = self._metadata_path(sensor_id, sensor_type)

        if os.path.exists(model_path):
            with open(model_path, "rb") as f:
                model = pickle.load(f)

        if os.path.exists(meta_path):
            with open(meta_path, "r") as f:
                meta = json.load(f)
                last_trained_time = datetime.fromisoformat(meta["last_trained_time"])

        return model, last_trained_time