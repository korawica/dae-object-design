import pandas as pd
from application.core.loader import (
    Connection,
    Catalog,
    Node,
    Schedule,
    Pipeline,
)


def test_connection_file():
    with Connection('demo:conn_local_data_landing').connect() as conn:
        print(conn.ls())
        print(conn.glob('*_csv*'))
        print(conn.exists('metadata.json'))
    # with Connection('demo:conn_aws_s3_data_eng').connect() as conn:
    #     print(conn.ls())
    #     print(conn.glob('demo*/*'))
    #     print(conn.ls('landing_data'))
    # with Connection('demo:conn_azure_blob_de_test').connect() as conn:
    #     print(conn.ls())
    #     print(conn.ls('dummy'))


def test_catalog_file_pandas():
    """Demo catalog"""

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    # pd.set_option('display.width', 150)
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('display.precision', 2)
    pd.set_option('display.max_seq_items', None)
    pd.set_option('display.max_colwidth', 500)

    def runner(name: str):
        print(name, '-' * 100)
        _df = Catalog(name).load()
        print(_df, '\n>')
        print(_df.dtypes)

    runner('demo:catl_customer_csv_type01')
    runner('demo:catl_customer_csv_type02')
    runner('demo:catl_customer_csv_type03')
    runner('demo:catl_customer_json_type01')
    runner('demo:catl_customer_json_type02')
    runner('demo:catl_customer_json_type03')
    runner('demo:catl_customer_json_type04')
    runner('demo:catl_customer_json_type05')
    runner('demo:catl_customer_json_type06')
    runner('demo:catl_customer_json_type07')
    runner('demo:catl_customer_json_type08')
    runner('demo:catl_customer_json_type09')
    runner('demo:catl_customer_excel_type01')
    runner('demo:catl_customer_excel_type02')


def test_connection_db():
    print('demo:conn_local_sqlite_bu01', '-' * 125)
    with Connection('demo:conn_local_sqlite_bu01').connect() as conn:
        rows = conn.execute('select 1;')
        result = rows.fetchall()
        print(result)
    print('demo:conn_pg_scgh_sandbox', '-' * 125)
    with Connection('demo:conn_pg_scgh_sandbox').connect() as conn:
        print(next(conn.tables()))
    print('demo:conn_pg_scgh_uat', '-' * 125)
    with Connection('demo:conn_pg_scgh_uat').connect() as conn:
        print(next(conn.tables(schema='ai')))


def test_catalog_sqlite():
    print('demo:catl_cntl_metadata', '-' * 125)
    df = Catalog('demo:catl_cntl_metadata', refresh=True).load(limit=5)
    print(df, '\n>')
    print(df.dtypes)
    print('demo:catl_sql_customer', '-' * 125)
    df = Catalog('demo:catl_sql_customer', refresh=True).load()
    print(df, '\n>')
    print(df.dtypes)


def test_schedule():
    _schedule = Schedule('demo:schd_for_node')
    print(_schedule.cronjob)
    _schedule_start = _schedule.generate('2021-01-01 00:00:00')
    print(_schedule_start.next)
    print(_schedule_start.next)
    print(_schedule_start.next)


if __name__ == '__main__':
    test_connection_file()
    # test_catalog_file_pandas()
    # test_schedule()
