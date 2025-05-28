import schedule
import time
import threading
import logging

from main import run_job

def run_scheduler(predict_days: int):
    logging.info(f"스케줄러 시작: {predict_days}일마다 예측 작업 실행 예약")

    # run_job에 predict_days 파라미터 넘기기
    schedule.every(predict_days).days.do(run_job, predict_days=predict_days)

    while True:
        try:
            schedule.run_pending()
            time.sleep(1000)
        except Exception as e:
            logging.error(f"스케줄러 실행 중 에러 발생: {e}", exc_info=True)

def start_scheduler_in_thread(predict_days: int):
    thread = threading.Thread(target=run_scheduler, args=(predict_days,), daemon=True)
    thread.start()
    logging.info("Scheduler Thread Start")