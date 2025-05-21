import requests
import logging

BASE_URL = "http://sensor-service"
VALID_STATES = {"pending", "completed", "abandoned"}

def get_sensor_list_by_state(state: str) -> list:
    if state not in VALID_STATES:
        raise ValueError(f"Invalid state: {state}")
    try:
        response = requests.get(f"{BASE_URL}/sensors", params={"state": state})
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"[SENSOR] 상태 조회 실패: {e}", exc_info=True)
        return []

def update_sensor_state(gateway_id: str, sensor_id: str, sensor_type: str, state: str):
    url = f"{BASE_URL}/gateways/{gateway_id}/sensors/{sensor_id}/types/{sensor_type}"
    try:
        response = requests.patch(url, json={"state": state})
        response.raise_for_status()
        logging.info(f"[SENSOR] 상태 변경 완료 → {state}")
    except requests.RequestException as e:
        logging.error(f"[SENSOR] 상태 변경 실패: {e}", exc_info=True)
