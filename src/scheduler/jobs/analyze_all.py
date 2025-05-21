import logging
import traceback
from datetime import datetime, timedelta

def analyze_all_sensors():
    logging.info("전체 센서 분석 시작")
    now = datetime.now(datetime.timezone.utc)

    try:
        sensors = get_sensor_list_by_state(["completed", "abandoned"])
    except Exception as e:
        logging.error(f"센서 목록 조회 실패: {e}", exc_info=True)
        return

    for sensor in sensors:
        sid = sensor.get("sensor_id", "UNKNOWN")
        try:
            gid = sensor["gateway_id"]
            stype = sensor["sensor_type"]

            meta = get_sensor_meta(gid, sid, stype)
            last_count = meta.get("last_data_count", 0)
            last_at_str = meta.get("last_analysis_at")

            # abandoned → pending 복구
            if sensor["state"] == "abandoned" and last_count > 0:
                update_sensor_state(sid, "pending")
                logging.info(f"[STATE] {sid} → pending (데이터 유입 확인)")
                continue

            # completed → 하루 경과한 경우만 재분석
            if last_at_str:
                last_at = datetime.fromisoformat(last_at_str)
                if now - last_at < timedelta(days=1):
                    continue

            # 재분석
            result = calculate_threshold_with_prophet(gid, sid, stype, duration="-1h")
            new_count = result.get("count", 0)

            if result["ready"]:
                handle_successful_analysis(gid, sid, stype, result)
            else:
                handle_failed_analysis(gid, sid, stype, new_count, result.get("reason"))

        except Exception as e:
            logging.error(f"{sid} 재분석 실패: {e}", exc_info=True)
