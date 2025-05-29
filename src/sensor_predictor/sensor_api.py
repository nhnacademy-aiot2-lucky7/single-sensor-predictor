import requests
import logging

BASE_URL = "http://localhost:10238"
VALID_STATES = "PENDING"

logger = logging.getLogger(__name__)

def load_sensor_list() -> list:

    url = f"{BASE_URL}/sensor-data-mappings/search-status?status={VALID_STATES}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        sensor_list = response.json()

        if not isinstance(sensor_list, list):
            logger.warning("[SENSOR] 비정상적인 응답 형식: list 아님")
            return []

        logger.info(f"[SENSOR] 상태가 '{VALID_STATES}' 센서 {len(sensor_list)}개 조회됨")
        return sensor_list
    except requests.RequestException as e:
        logging.error(f"[SENSOR] 상태 조회 실패: {e}", exc_info=True)
        return []