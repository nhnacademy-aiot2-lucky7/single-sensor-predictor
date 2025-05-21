import os
import pickle

class LocalStorage:
    def __init__(self, base_dir="./models"):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    def save_model(self, model, sensor_id, base_date):
        save_path = os.path.join(self.base_dir, base_date.strftime("%Y-%m-%d"))
        os.makedirs(save_path, exist_ok=True)
        filename = f"model_{sensor_id}_{base_date.strftime('%Y%m%d')}.pkl"
        filepath = os.path.join(save_path, filename)
        with open(filepath, "wb") as f:
            pickle.dump(model, f)