import logging
from influxdb_client import InfluxDBClient

logger = logging.getLogger(__name__)

class InfluxService:
    # 상수 정의
    FIELD_GATEWAY_ID = "gateway-id"
    FIELD_SENSOR_ID = "sensor-id"
    FIELD_SENSOR_TYPE = "_field"
    MEASUREMENT = "sensor-data"

    def __init__(self, url, token, org, bucket):
        try:
            self.client = InfluxDBClient(url=url, token=token, org=org)
            self.query_api = self.client.query_api()
            self.bucket = bucket
            logger.info("[InfluxService] 초기화 완료")
        except Exception as e:
            logger.error(f"[InfluxService] 초기화 실패: {e}", exc_info=True)
            raise

    def close(self):
        try:
            if self.client:
                self.client.close()
                logger.info("[InfluxService] InfluxDBClient 닫힘")
        except Exception as e:
            logger.warning(f"[InfluxService] 클라이언트 닫는 중 예외 발생: {e}")

    def __del__(self):
        self.close()

    def get_sensor_metadata(self, duration, completed_sensor_list):

        try:
            tables = self.query_api.query(f'''
        import "influxdata/influxdb/schema"
        schema.tagValues(bucket: "{self.bucket}", tag: "{self.FIELD_SENSOR_ID}")
        ''')
            if not tables:
                logger.warning("InfluxDB에서 sensor-id를 찾을 수 없습니다.")
                return []

            sensor_ids = [r.get_value() for r in tables[0].records]

            # ✅ 안전하게 completed_ids 추출
            completed_ids = []
            if completed_sensor_list:
                if isinstance(completed_sensor_list[0], dict):
                    completed_ids = [s["sensor_id"] for s in completed_sensor_list if "sensor_id" in s]
                else:
                    completed_ids = completed_sensor_list

            logger.debug(f"sensor_ids from influx: {sensor_ids}")
            logger.debug(f"filtered completed_ids: {completed_ids}")

            sensor_ids = list(set(sensor_ids) & set(completed_ids))
            logger.info(f"sensor-ids after intersection: {sensor_ids}")

            sensor_meta = []
            for sid in sensor_ids:
                detail_query = f'''
                from(bucket: "{self.bucket}")
                    |> range(start: {duration})
                    |> filter(fn: (r) => r["_measurement"] == "{self.MEASUREMENT}")
                    |> filter(fn: (r) => r["{self.FIELD_SENSOR_ID}"] == "{sid}")
                    |> keep(columns: ["{self.FIELD_GATEWAY_ID}", "{self.FIELD_SENSOR_ID}", "{self.FIELD_SENSOR_TYPE}"])
                    |> group(columns: ["{self.FIELD_GATEWAY_ID}", "{self.FIELD_SENSOR_TYPE}"])
                    |> distinct(column: "{self.FIELD_SENSOR_TYPE}")
                '''
                result_tables = self.query_api.query(detail_query)
                for table in result_tables:
                    for record in table.records:
                        sensor_meta.append({
                            self.FIELD_SENSOR_ID: sid,
                            self.FIELD_GATEWAY_ID: record.values.get(self.FIELD_GATEWAY_ID),
                            "sensor_type": record.get_field()
                        })

            logger.info(f"sensor_meta count: {len(sensor_meta)}")
            return sensor_meta
        except Exception as e:
            logger.error(f"[InfluxService] 센서 메타데이터 조회 실패: {e}", exc_info=True)
            return []

    def load_sensor_data(self, sensor_id, gateway_id, sensor_type, duration):
        try:
            query = f'''
        from(bucket: "{self.bucket}")
            |> range(start: {duration})
            |> filter(fn: (r) => r["_measurement"] == "{self.MEASUREMENT}")
            |> filter(fn: (r) => r["{self.FIELD_GATEWAY_ID}"] == "{gateway_id}")
            |> filter(fn: (r) => r["{self.FIELD_SENSOR_ID}"] == "{sensor_id}")
            |> filter(fn: (r) => r["{self.FIELD_SENSOR_TYPE}"] == "{sensor_type}")
            |> keep(columns: ["_time", "_value"])
            |> rename(columns: {{"_time": "ds", "_value": "y"}})
            |> sort(columns: ["ds"])
        '''
            tables = self.query_api.query(query)
            data = []
            for table in tables:
                for record in table.records:
                    try:
                        dt = record["ds"]
                        timestamp = dt.timestamp()
                        x = {"ds": timestamp}
                        y = record["y"]

                        data.append((x,y))
                    except Exception as e:
                        logger.warning(f"⚠️ 레코드 파싱 실패: {record.values} - {e}")
            return data
        except Exception as e:
            logger.error(f"[InfluxService] 센서 데이터 로딩 실패 (sensor-id={sensor_id}): {e}", exc_info=True)
            return []