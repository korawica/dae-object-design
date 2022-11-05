Data Application Engine: *Object Design*
========================================

The project data application engine in part of **object design**. This project want to create base concept object
template of ETL/ELT process for any OLTP/OLAP system.

### Main components

 - [Formatter](#Formatter)
 - [Register](#Register)
 - [Loader](#Loader)
    - [Connection](#*Connection*)
    - [Catalog](#*Catalog*)
    - [Node](#*Node*)
    - [Schedule](#*Schedule*)    
    - [Pipeline](#~~*Pipeline*~~)

> **Note:**
> ...

---

Formatter
---------

The formatter component implement formatter objects for parse and format with base value, such as
`Datetime`, `Version`, and `Serial` objects. This component was used for parse any filename with setting format
string value in the register component.

The formatter can enhancement any format value from sting value, like in `Datetime`, for `%B` value was designed
to month shortname (`Jan`, `Feb`, etc.) 

```python
from application.core.formatter import Datetime

datetime = Datetime.parse(
   value='This_is_time_20220101_000101',
   fmt='This_is_time_%Y%m%d_%H%M%S'
)
datetime.format('New_format_%Y%b-%-d_%H:%M:%S')
```

```text
>>> 'New_format_2022Jan-1_00:01:01'
```

---

Register
--------

The register component have main objective for control config version and backup data from the base config data
store or data lake. Once the configuration name was registered, the data will be loaded into the stage area that
set on `parameter.yaml` file like below example.


```yaml
# ./parameter.yaml
stages:
    staging:
        format: "{name:%n}.{timestamp:%Y%m%d_%H%M%S}"
    persisted:
        format: "{name:%n}.{version:v%m.%n.%c}"
    curated:
        format: "{domain:%n}_{name:%n}.{compress:%-g}"
        rules:
            compress: "gzip"
```

If config file in the base data store exists with this path, 

```text
conf
 └─── demo
      └─── control_demo_file.yaml  # This file contain `control_demo_name` data.
```

and deploy data to all stage in setting file with statement,

```python
from application.core.register import Register

data = Register('demo:control_demo_name')
data.deploy()
```

the registered data will move to the stages,

```text
data/.conf
 ├─── staging
 │    └─── control_demo_name.20220101_000000.json
 ├─── persisted
 │    └─── control_demo_name.0.0.1.json
 └─── curated
      └─── demo_control_demo_name.gz.json
```

> **Note:**
> Register object implement data management by retention method for purge data in any stage area
> with rules like `{'timestamp': 15, 'timestamp_metric': 'minutes''}` or `{'version': '0.3.3'}`.

---

Loader
------

The loader component implement loader object that load configuration data from the final stage of registration,
such as a `curated` stage.

#### Main loader objects
 
 - [Connection](#*Connection*)
 - [Catalog](#*Catalog*)
 - [Node](#*Node*)
 - [Schedule](#*Schedule*)
 - [Pipeline](#~~*Pipeline*~~)

### *Connection*

The connection object keep any endpoint of source and target connections for the catalog object. A endpoint can
be any system like local file system, relational database system, or container storage system which implementable
with vendor python library.

Any connection objects should inherit from base connection object that provide necessary connection properties
and methods for get and put any data with the connection. If a connection object was local file system, it will
have searching methods like `ls`, or `walk` for search any files in a connection endpoint, local path url.

  - **File System**
    
    The file system object will implement `ls`, `glob`, or `walk` methods.
    
    ```yaml
    connection_local_file_landing:
        type: "connection.LocalSystem"
        endpoint: "file:///${APP_PATH}/data/demo/landing"
    ```

    ```python
    from application.core.loader import Connection
    
    with Connection('demo:connection_local_file_landing').connect() as conn:
        conn.glob('*_csv*')
    ```
    
    ```text
    >>> ['${APP_PATH}/data/demo/landing/customer_csv.type01.csv',
    >>> '${APP_PATH}/data/demo/landing/customer_csv.type02.csv',
    >>> '${APP_PATH}/data/demo/landing/customer_csv.type03.csv',
    >>> '${APP_PATH}/data/demo/landing/seller_csv.type01.csv',
    >>> '${APP_PATH}/data/demo/landing/seller_prepare_csv.type01.20210101.csv',
    >>> '${APP_PATH}/data/demo/landing/seller_prepare_csv.type01.20221017.csv']
    ```

  - **Relational Database System**
 
    This system will implement interaction method, like tables in database listing function.

    ```yaml
    connection_local_sqlite_metadata:
        type: "connection.SQLiteSystem"
        endpoint: "sqlite:///${APP_PATH}/data/metadata.db"
    ```

    ```python
    from application.core.loader import Connection
    
    conn = Connection('demo:connection_local_sqlite_metadata')
    with Connection('demo:conn_pg_scgh_sandbox').connect() as conn:
        conn.tables()
    ```
    
    ```text
    >>> [tbl_metadata, ]
    ```

### *Catalog*

The catalog object keep profile catalog of data for loading and saving to related connection object. By default,
this project implement returning data type with Pandas DataFrame.

Catalog template file can define the schema for data schema validation.

```yaml
catalog_customer:
    type: "catalog.PandasCSVFile"
    connection: "demo:conn_local_data_landing"
    endpoint: "customer_csv.type01.csv"
    schemas:
        customer_id: {alias: "id::int", nullable: false}
        customer_name: {alias: "name::str", nullable: true}
        customer_age: {alias: "age::str", nullable: true}
        phone_number: {alias: "phone::str", nullable: true}
        register_date: {alias: "datetime64", nullable: false}
        active_flag: {alias: "active::bool", nullable: false}
    encoding: "utf-8"
    delimiter: "|"
    header: 0
    quoting: 3
```

```python
from application.core.loader import Catalog

Catalog('demo:catalog_customer').load()
```

```text
>>>    customer_id   customer_name customer_age phone_number register_date  active_flag
>>> 0            1  John@email.com          NaN      01-1341    2022-01-01         True
>>> 1            2    Sara Toronto           37      01-2201    2022-01-01         True
>>> 2            3             NaN          NaN      04-1772    2022-01-01        False
>>> 3            4        Tome Vee           15      02-1821    2022-01-01        False
>>> 4            5           Vimmy           23      08-2215    2022-01-01         True
>>> 5            6        Queen J.           19      01-1003    2022-01-01         True 
```

> **Note:**
> The catalog object will work around with DataFrame engine libraries, such as `pandas`, `dask`, or `polar`, because 
> this engine can transform data by aggregate methods.

### *Node*

The node object keep transform process template. The template include `input`, `transform`, and `output` keys.
ฺThis project implements Pandas and RDBMS node template.

In a node object, it contains action objects that define transformation action such as `GroupBy`, `Filter`, or 
`RenameColumn`.

  - **Prepare Data with link catalog template**

    ```yaml
    node_seller_prepare:
        type: 'node.PandasNode'
        input:
            - alias: "seller"
              from: "demo:catalog_seller_csv"
        transform:
            - alias: "seller_prepare"
              input: "seller"
              actions:
                  - type: "GroupBy"
                    columns: ['customer_id', 'product_id']
                    aggregate:
                        order: "('document_date', 'count')"
                        value_max: "('sales_value', 'max')"
                        value_margin: "('sales_value', 'lambda x: x.max() - x.min()')"
                  - type: "RenameColumn"
                    columns:
                        order: "order_sales"
                  - type: "Filter"
                    condition: 'order_sales >= 2'
            - alias: "seller_dq"
              input: "seller_prepare"
              actions:
                  - type: "DataQuality"
                    dq_function: "is_null"
                    columns: ["customer_id"]
                  - type: "DataQuality"
                    dq_function: "outlier"
                    columns: ["value_margin"]
                    options:
                        std_value: 3
        output:
            - from: "seller_prepare"
              to: "demo:catalog_seller_csv_prepare"
              mode: "overwrite"
    ```
    
    ```python
    from application.core.loader import Node
    
    node = Node('node_seller_prepare')
    node.deploy()
    ```
    
    ```text
    >>> This task: 'seller_prepare' will running in action mode ...
    >>> Start action: GroupBy ...
    >>>     customer_id product_id  order  value_max  value_margin
    >>> 0             1        00A      2      300.0         280.0
    >>> 1             1        00B      1      300.0           0.0
    >>> 2             1        00C      1       75.0           0.0
    >>> 3             2        00A      1      300.0           0.0
    >>> 4             2        00B      2      250.0         150.0
    >>> 5             2        00C      1      105.0           0.0
    >>> 6             2        00D      1       15.0           0.0
    >>> 7             3        00B      1      550.0           0.0
    >>> 8             3        00C      1       60.0           0.0
    >>> 9             3        00D      1      300.0           0.0
    >>> 10            4        00A      2      300.0         270.0
    >>> 11            4        00B      2      200.0         150.0
    >>> 12            5        00C      1       30.0           0.0
    >>> 13            6        00B      1       50.0           0.0
    >>> Start action: RenameColumn ...
    >>>     customer_id product_id  order_sales  value_max  value_margin
    >>> 0             1        00A            2      300.0         280.0
    >>> 1             1        00B            1      300.0           0.0
    >>> 2             1        00C            1       75.0           0.0
    >>> 3             2        00A            1      300.0           0.0
    >>> 4             2        00B            2      250.0         150.0
    >>> 5             2        00C            1      105.0           0.0
    >>> 6             2        00D            1       15.0           0.0
    >>> 7             3        00B            1      550.0           0.0
    >>> 8             3        00C            1       60.0           0.0
    >>> 9             3        00D            1      300.0           0.0
    >>> 10            4        00A            2      300.0         270.0
    >>> 11            4        00B            2      200.0         150.0
    >>> 12            5        00C            1       30.0           0.0
    >>> 13            6        00B            1       50.0           0.0
    >>> Start action: Filter ...
    >>>     customer_id product_id  order_sales  value_max  value_margin
    >>> 0             1        00A            2      300.0         280.0
    >>> 4             2        00B            2      250.0         150.0
    >>> 10            4        00A            2      300.0         270.0
    >>> 11            4        00B            2      200.0         150.0
    ```
    
    ```text
    >>> This task: 'seller_dq' will running in action mode ...
    >>> Start action: DataQuality ...
    >>>     customer_id product_id  ...  value_margin  customer_id_dq_isnull
    >>> 0             1        00A  ...         280.0                  False
    >>> 4             2        00B  ...         150.0                  False
    >>> 10            4        00A  ...         270.0                  False
    >>> 11            4        00B  ...         150.0                  False
    >>> Start action: DataQuality ...
    >>>     customer_id product_id  ...  customer_id_dq_isnull  value_margin_dq_outlier
    >>> 0             1        00A  ...                  False                    False
    >>> 4             2        00B  ...                  False                    False
    >>> 10            4        00A  ...                  False                    False
    >>> 11            4        00B  ...                  False                    False
    ```

  - **Transform Data with full manual template**

    ```yaml
    node_seller_transform:
        type: 'node.PandasNode'
        input:
            - alias: "seller"
              from:
                  type: "catalog.PandasCSVFile"
                  connection:
                      type: "connection.LocalSystem"
                      endpoint: "file:///${APP_PATH}/data/demo/landing"
                  endpoint: "seller_prepare_csv.type01.%Y%m%d.csv"
                  encoding: "utf-8"
                  header: 0
                  delimiter: "|"
            - alias: "customer"
              from:
                  type: "catalog.PandasCSVFile"
                  connection:
                      type:     "connection.LocalSystem"
                      endpoint: "file:///${APP_PATH}/data/demo/landing"
                  endpoint: "customer_csv.type01.csv"
                  encoding: "utf-8"
                  escapechar: "\\"
                  delimiter: "|"
                  header: 0
                  quoting: 3
                  true_values: ["Yes"]
                  false_values: ["No", "false"]
        transform:
            - alias: "seller_map_customer"
              input: "seller"
              actions:
                  - type: "RenameColumn"
                    columns:
                        value_margin: "margin"
                  - type: "GroupBy"
                    columns: ['customer_id']
                    aggregate:
                        total_order: "('order_sales', 'sum')"
                        value_max: "('value_max', 'max')"
                        total_margin: "('margin', 'sum')"
                  - type: "Join"
                    other: 'customer'
                    'on': ['customer_id']
                    how: 'left'
                  - type: "SelectColumn"
                    columns: ['customer_id', 'name', 'phone', 'total_order', 'value_max', 'total_margin']
        output:
            - from: "seller_map_customer"
              to:
                  type: "catalog.PandasCSVFile"
                  connection:
                      type: "connection.LocalSystem"
                      endpoint: "file:///${APP_PATH}/data/demo/landing"
                  endpoint: "seller_map_customer_%Y%m%d.csv"
                  encoding: "utf-8"
                  header: true
                  delimiter: "|"
              mode: "append"
    ```
    
    ```python
    from application.core.loader import Node
    
    node = Node('node_seller_transform')
    node.deploy()
    ```
    
    ```text
    >>> This task: 'seller_map_customer' will running in action mode ...
    >>> Start action: RenameColumn ...
    >>>    customer_id product_id  order_sales  value_max  margin
    >>> 0            1        00A            2      300.0   280.0
    >>> 1            2        00B            2      250.0   150.0
    >>> 2            4        00A            2      300.0   270.0
    >>> 3            4        00B            2      200.0   150.0
    >>> Start action: GroupBy ...
    >>>    customer_id  total_order  value_max  total_margin
    >>> 0            1            2      300.0         280.0
    >>> 1            2            2      250.0         150.0
    >>> 2            4            4      300.0         420.0
    >>> Start action: Join ...
    >>>    customer_id  total_order  value_max  total_margin  id          name   age    phone register_date  active
    >>> 0            1            2      300.0         280.0   2  Sara Toronto  37.0  01-2201    2022-01-01    True
    >>> 1            2            2      250.0         150.0   3           NaN   NaN  04-1772    2022-01-01   False
    >>> 2            4            4      300.0         420.0   5         Vimmy  23.0  08-2215    2022-01-01    True
    >>> Start action: SelectColumn ...
    >>>    customer_id          name    phone  total_order  value_max  total_margin
    >>> 0            1  Sara Toronto  01-2201            2      300.0         280.0
    >>> 1            2           NaN  04-1772            2      250.0         150.0
    >>> 2            4         Vimmy  08-2215            4      300.0         420.0
    ```

> **Note:**
> The Config YAML loader object use safe mode, so data in `.yaml` file with value `ture`, `false`, `yes`,
> `no`, `on`, and `off` will convert to the Boolean type. If the data in file does not want to convert, 
> it will add quote to that value.

### *Schedule*

The schedule object keep cron schedule value, standard schedule value for any common system.

```yaml
schd_for_node:
    type: 'schedule.BaseSchedule'
    cron: "*/5 * * * *"
```

```python
from application.core.loader import Schedule

schedule = Schedule('schd_for_node')
schedule.cronjob
```

```text
>>> '*/5 * * * *'
```

```python
cron_iterate = schedule.generate('2022-01-01 00:00:00')
for _ in range(5):
   cron_iterate.next.strftime('%Y-%m-%d %H:%M:%S')
```

```text
>>> 2022-01-01 00:05:00
>>> 2022-01-01 00:10:00
>>> 2022-01-01 00:15:00
>>> 2022-01-01 00:20:00
>>> 2022-01-01 00:25:00
```

> **Note:**
> The cronjob schedule iterator can use `prev` for move backward of datetime.

### ~~*Pipeline*~~

The pipeline object does not implement in this project yet.

---

License
-------

This project was licensed under the terms of the [MIT license](LICENSE.md).