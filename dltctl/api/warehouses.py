from databricks import sql
from tabulate import tabulate
import pandas as pd
import pyarrow as pa

import os

class NoRunningWarehouseFoundError(Exception):
    pass


class DBSQLClient():
    def __init__(self, api_client, sql_client=None, sql_warehouse=None, hostname=None, token=None, headers=None):
        self.client = api_client
        self.token = token if token else api_client.default_headers["Authorization"].split(' ')[1]
        self.headers = headers if headers else api_client.default_headers
        self.sql_client = sql_client
        self.sql_warehouse = sql_warehouse
        self.server_hostname = None
        self.http_path = None
        self.cursor = None

    def list_warehouses(self):
        warehouses = self.client.perform_query(
            'GET', f'/sql/warehouses', headers=self.headers)
        return warehouses["warehouses"]

    
    def get_warehouse(self, warehouse_id):
        warehouse = self.client.perform_query(
            'GET', f'/sql/warehouses/{warehouse_id}', headers=self.headers)
        return warehouse

    def _pick_client_warehouse(self):
        warehouses = self.list_warehouses()
        warehouse_details = []
        for w in warehouses:
            warehouse = self.get_warehouse(w["id"])
            status = warehouse["state"]
            # Pick the first running one for now
            # TODO - optimially pick a good one
            if status == 'RUNNING':
                odbc_params = warehouse["odbc_params"]
                self.server_hostname = odbc_params["hostname"]
                self.http_path = odbc_params["path"]
                return warehouse
        # If no running warehouses to use, throw an exception for now. 
        # TODO - Pick one to start if needed
        raise NoRunningWarehouseFoundError("A running warehouse that you have access to is required to execute queries")
         

    def connect(self):
        if not self.sql_client:
            w = self._pick_client_warehouse()
            client = sql.connect(self.server_hostname, self.http_path, self.token)
            self.sql_client = client
            return self.sql_client
        else:
            return self.sql_client
    def disconnect(self):
        if self.sql_client:
            self.sql_client.close()
            self.sql_client = None
        return
    
    def query(self, query):
        self.connect()
        if not self.cursor:   
          self.cursor = self.sql_client.cursor()

        self.cursor.execute(f"{query}")
        results = self.cursor.fetchall_arrow()
        df = results.to_pandas()
        print(tabulate(df, headers = 'keys', tablefmt = 'psql'))
        self.disconnect()
        return df

        





    



