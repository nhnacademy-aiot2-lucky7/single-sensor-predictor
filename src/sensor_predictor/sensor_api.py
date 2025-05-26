import requests
import logging

BASE_URL = "http://sensor-service"
VALID_STATES = {"pending", "completed", "abandoned"}

logger = logging.getLogger(__name__)

def get_sensor_list_by_state(state: str) -> list:
    if state not in VALID_STATES:
        raise ValueError(f"Invalid state: {state}")

    url = f"{BASE_URL}/sensors"
    try:
        response = requests.get(url, params={"state": state}, timeout=5)
        response.raise_for_status()
        sensor_list = response.json()

        if not isinstance(sensor_list, list):
            logger.warning("[SENSOR] 비정상적인 응답 형식: list 아님")
            return []

        logger.info(f"[SENSOR] 상태가 '{state}'인 센서 {len(sensor_list)}개 조회됨")
        return sensor_list
    except requests.RequestException as e:
        logging.error(f"[SENSOR] 상태 조회 실패: {e}", exc_info=True)
        return []

def update_sensor_state(gateway_id: str, sensor_id: str, sensor_type: str, state: str):
    if state not in VALID_STATES:
        raise ValueError(f"Invalid state: {state}")

    url = f"{BASE_URL}/gateways/{gateway_id}/sensors/{sensor_id}/types/{sensor_type}"
    try:
        response = requests.patch(url, json={"state": state}, timeout=5)
        response.raise_for_status()
        logger.info(f"[SENSOR] 센서 상태 변경 완료: {sensor_id} → {state}")
    except requests.RequestException as e:
        logger.error(f"[SENSOR] 센서 상태 변경 실패: {sensor_id}, error: {e}", exc_info=True)
