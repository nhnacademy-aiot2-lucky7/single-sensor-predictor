import logging
from influxdb_client import InfluxDBClient
from datetime import time

logger = logging.getLogger(__name__)

class InfluxService:
    def __init__(self, url, token, org, bucket):
        try:
            self.client = InfluxDBClient(url=url, token=token, org=org)
            self.query_api = self.client.query_api()
            self.bucket = bucket
            logger.info("[InfluxService] 초기화 완료")
        except Exception as e:
            logger.error(f"[InfluxService] 초기화 실패: {e}", exc_info=True)
            raise

    def get_sensor_metadata(self, duration, completed_sensor_list):
        try:
            tables = self.query_api.query(f'''
            import "influxdata/influxdb/schema"
            schema.tagValues(bucket: "{self.bucket}", tag: "sensor_id")
            ''')
            if not tables:
                logger.warning("InfluxDB에서 sensor_id를 찾을 수 없습니다.")
                return []

            sensor_ids = [r.get_value() for r in tables[0].records]
            completed_ids = [s["sensor_id"] for s in completed_sensor_list] if completed_sensor_list and isinstance(completed_sensor_list[0], dict) else completed_sensor_list
            sensor_ids = list(set(sensor_ids) & set(completed_ids))

            sensor_meta = []
            for sid in sensor_ids:
                detail_query = f'''
                from(bucket: "{self.bucket}")
                    |> range(start: {duration})
                    |> filter(fn: (r) => r["_measurement"] == "sensor-data")
                    |> filter(fn: (r) => r["sensor_id"] == "{sid}")
                    |> keep(columns: ["gateway_id", "sensor_id", "_field"])
                    |> group(columns: ["gateway_id", "_field"])
                    |> distinct(column: "_field")
                '''
                tables = self.query_api.query(detail_query)
                for table in tables:
                    for record in table.records:
                        sensor_meta.append({
                            "sensor_id": sid,
                            "gateway_id": record.values.get("gateway_id"),
                            "sensor_type": record.get_field()
                        })

            return sensor_meta
        except Exception as e:
            logger.error(f"[InfluxService] 센서 메타데이터 조회 실패: {e}", exc_info=True)
            return []

    def load_sensor_data(self, sensor_id, gateway_id, sensor_type, duration):
        try:
            query = f'''
            from(bucket: "{self.bucket}")
                |> range(start: {duration})
                |> filter(fn: (r) => r["_measurement"] == "sensor-data")
                |> filter(fn: (r) => r["gateway_id"] == "{gateway_id}")
                |> filter(fn: (r) => r["sensor_id"] == "{sensor_id}")
                |> filter(fn: (r) => r["_field"] == "{sensor_type}")
                |> keep(columns: ["_time", "_value"])
                |> rename(columns: {{"_time": "ds", "_value": "y"}})
                |> sort(columns: ["ds"])
            '''
            tables = self.query_api.query(query)
            data = []
            for table in tables:
                for record in table.records:
                    ts = record.get_time()
                    # ⏰ 시간 필터: 9시 ~ 18시
                    if time(9, 0) <= ts.time() <= time(18, 0):
                        data.append((ts, record.get_value()))
            return data
        except Exception as e:
            logger.error(f"[InfluxService] 센서 데이터 로딩 실패 (sensor_id={sensor_id}): {e}", exc_info=True)
            return []