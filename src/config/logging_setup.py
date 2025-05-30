import os
import logging
from dotenv import load_dotenv

def setup_logging():
    load_dotenv()

    log_file = os.getenv("LOGGING_FILE_NAME", "logs/single-sensor-predictor.log")
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # 이미 핸들러가 등록되어 있다면 건너뜀 (중복 설정 방지)
    if len(logging.getLogger().handlers) == 0:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        logging.getLogger(__name__).info("✅ 로깅 설정 완료")