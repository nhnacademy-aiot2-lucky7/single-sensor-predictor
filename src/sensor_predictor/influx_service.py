from influxdb_client import InfluxDBClient

class InfluxService:
    def __init__(self, url, token, org, bucket):
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.query_api = self.client.query_api()
        self.bucket = bucket

    def get_sensor_metadata(self, duration, completed_sensor_list):
        tables = self.query_api.query(f'''
        import "influxdata/influxdb/schema"
        schema.tagValues(bucket: "{self.bucket}", tag: "sensor_id")
        ''')
        if not tables:
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

    def load_sensor_data(self, sensor_id, gateway_id, sensor_type, duration):
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
                data.append((record.get_time(), record.get_value()))
        return data
