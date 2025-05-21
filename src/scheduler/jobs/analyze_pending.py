import logging
from notifier.notifier import notify_rule_engine
from storage.local_storage import is_gateway_notified, mark_gateway_as_notified
from service.sensor_service import (
    get_sensor_list_by_state,
    get_sensor_list_by_gateway_id,
    get_all_gateway_id
)
from service.threshold_service import (
    calculate_threshold_with_prophet,
    handle_successful_analysis,
    handle_failed_analysis
)

# pending 상태의 센서들만 분석
def analyze_pending_sensors():
    logging.info("Pending 센서 분석 시작")

    try:
        pending_sensors = get_sensor_list_by_state("pending")
    except Exception as e:
        logging.error(f"센서 목록 조회 실패: {e}", exc_info=True)
        return  # 전체 분석 스킵, 모듈은 유지

    for sensor in pending_sensors:
        try:
            gid = sensor["gateway_id"]
            sid = sensor["sensor_id"]
            stype = sensor["sensor_type"]

            result = calculate_threshold_with_prophet(gid, sid, stype, duration="-1h")
            count = result.get("count", 0)

            if result["ready"]:
                handle_successful_analysis(gid, sid, stype, result)
            else:
                handle_failed_analysis(gid, sid, stype, count, result.get("reason"))

        except Exception as e:
            logging.error(f"{sensor['sensor_id']} 분석 실패: {str(e)}", exc_info=True)

    # 게이트웨이별로 분석 완료 여부 확인 후 통보
    for gid in get_all_gateway_id():
        if is_gateway_analysis_completed(gid) and not is_gateway_notified(gid):
            notify_rule_engine(gid)
            mark_gateway_as_notified(gid)

# 게이트웨이의 센서들 중 pending이 있는지 확인
def is_gateway_analysis_completed(gateway_id) -> bool:
    sensors = get_sensor_list_by_gateway_id(gateway_id)
    return not any(sensor["state"] == "pending" for sensor in sensors)
