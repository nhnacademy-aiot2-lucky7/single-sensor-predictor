import os
import logging
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 기존 루트 핸들러 제거 (중복 방지)
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# 로그 파일 경로 및 로그 레벨 가져오기
log_file = os.getenv("LOGGING_FILE_NAME", "logs/single-sensor-predictor.log")
log_level_str = os.getenv("LOGGING_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

# 로그 디렉토리 없으면 생성
os.makedirs(os.path.dirname(log_file), exist_ok=True)

# 로깅 설정
logging.basicConfig(
    level=log_level,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()  # 콘솔 출력도 병행
    ]
)

# 사용 예시
logger = logging.getLogger(__name__)
logger.info("🔧 로그 설정이 완료되었습니다.")