import schedule
import time
import threading
import logging

from main import run_job

def run_scheduler():
    logging.info("스케줄러 시작: 5일마다 예측 작업 실행 예약")

    schedule.every(5).days.do(run_job)

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            logging.error(f"스케줄러 실행 중 에러 발생: {e}", exc_info=True)

def start_scheduler_in_thread():
    thread = threading.Thread(target=run_scheduler, daemon=True)
    thread.start()
    logging.info("스케줄러 쓰레드 시작됨")