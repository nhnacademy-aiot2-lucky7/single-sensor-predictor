import schedule
import time
import threading
import logging

from main import run_job

def run_scheduler():
    schedule.every(5).days.do(run_job())  # 5일마다 job 실행

    while True:
        schedule.run_pending()
        time.sleep(1)

def start_scheduler_in_thread():
    thread = threading.Thread(target=run_scheduler, daemon=True)
    thread.start()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    start_scheduler_in_thread()
    while True:
        time.sleep(60)
