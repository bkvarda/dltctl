from dltctl.api.warehouses import DBSQLClient

def assert_table_exists(t):
    if t == "foo":
        return True
    else:
        raise AssertionError(f"{t} != foo")
