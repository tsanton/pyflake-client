# Pyflake Client

A testing utility to deterministically test snowflake implementations.

**NOTE**: Do not use this as a client library to execute DDL, DML or DCL in a production environment. \
This code is highly suseptible to SQL-injection. This library is intended to generate & describe ephemeral assets which can (and probably will) be dropped without notice.

## Install

```sh
# From master
pip install git+https://github.com/Tsanton/pyflake-client#egg=pyflake-client
# From branch
pip install git+https://github.com/Tsanton/pyflake-client@master#egg=pyflake-client
# From tag
pip install git+https://github.com/Tsanton/pyflake-client@v0.1.0#egg=pyflake-client
```

**requirements.txt**
```
package-one==1.9.4
git+https://github.com/path/to/package-two@v1.0.0#egg=package-two
package-three==1.0.1
```

## Usage

### execute_async
Executes a SQL statement asynchronously.

```python
job = flake.execute_async("CALL PROCEDURE x.y.MYPROC();")
```

This return a [AsynCallJob](./pyflake_client/async_call_job.py) which then must have one of the following functions invoked:

**wait():**

```python
job = flake.execute_async("CALL PROCEDURE x.y.MYPROC();")
job.wait()
```
This will block until the job is completed

**fetch_one():**

By providing the class you want to deserialize your response into, and a deserializer for how to parse the response, you can fetch_one()

```python
job = flake.execute_async("SELECT 'Hello you' AS GREETING;")
greeting = job.fetch_one(str, lambda x : str(x))
```
This will block until the job is completed

**fetch_many():**

By providing the class you want to deserialize your response into, and a deserializer for how to parse the response, you can fetch_many()
```python
job = flake.execute_async("SELECT 'Hello Foo' AS GREETING union all SELECT 'Hello Bar' AS GREETING;")
greetings = job.fetch_many(str, lambda x : str(x))
```
This will block until the job is completed

**is_done():**

```python
job = flake.execute_async("CALL PROCEDURE x.y.MYPROC();")
if job.is_done():
    print("Done")
else:
    print("Not done")
```
**cancel():**
```python
job = flake.execute_async("CALL PROCEDURE x.y.MYPROC();")
job.cancel()
```
This will block until the cancellation of the job is completed


### create_asset_async

Creates a Snowflake asset asynchronously.\
An asset is defined as an object in snowflake, be it an accout level entity (database/role), or schema level entity (table or function).

In order to be an asset your dataclass must implement the [ISnowflakeAsset](./pyflake_client/models/assets/snowflake_asset_interface.py).

The simplest example is the [database](./pyflake_client/models/assets/database.py) asset, note it's implementation of `get_create_statement` and `get_delete_statement`:

```python
@dataclass(frozen=True)
class Database(ISnowflakeAsset):
    db_name: str
    comment: str
    owner: ISnowflakePrincipal

    #implements ISnowflakeAsset
    def get_create_statement(self):
        if self.owner is None:
            raise ValueError("Create statement not supported for owner-less databases")
        snowflake_principal_type = self.owner.get_snowflake_type().snowflake_type()
        if snowflake_principal_type not in ["ROLE"]:
            raise NotImplementedError("Ownership is not implemented for asset of type {self.owner.__class__}")

        return f"""CREATE OR REPLACE DATABASE {self.db_name} COMMENT = '{self.comment}';
                   GRANT OWNERSHIP ON DATABASE {self.db_name} TO {snowflake_principal_type} {self.owner.get_identifier()};"""

    #implements ISnowflakeAsset
    def get_delete_statement(self):
        return f"drop database if exists {self.db_name} CASCADE;"

```

**Here is how you use create_asset_async:**

```python
asset = Database(db_name="demo_db", comment="some_comment", owner=Role(name="SYSADMIN")) 
job = flake.create_asset_async(asset)
```

This return a [AsynAssetJob](./pyflake_client/async_asset_job.py) which then must be one of:

**wait():**

```python
job = flake.create_asset_async(asset)
job.wait()
```
This will block until the job is completed

**cancel():**
```python
job = flake.create_asset_async(asset)
job.cancel()
```
This will block until the cancellation of the job is completed


### register_asset_async
Registers a Snowflake asset and adds it to a queue.

This is an extension of [create_asset_async](#create_asset_async) as it allows you to add this asset to a Last In First Out (LIFO) queue, facilitating easy cleanup:

```python
asset_queue = queue.LifoQueue()
try:
    job = flake.register_asset_async(asset, asset_queue)
    job.wait()
    #Create more assets, query out logic etc etc
    ...
finally:
    ### Will delete your registered assets in reversed order to creation (i.e. last created is deleted first)
    flake.delete_assets(asset_queue)
```

### delete_asset_async
Deletes a Snowflake asset asynchronously.

```python
job = flake.delete_asset_async(asset)
job.wait()  # Wait for the asset to be deleted
job.cancel() # Or cancel the creation.
```

### delete_assets
Deletes multiple Snowflake assets.

The cleanup-function for [register_asset_async](#register_asset_async)

```python
asset_queue = queue.LifoQueue()  # Queue containing assets to be deleted
flake.delete_assets(asset_queue)
```

### describe_async
Describes a Snowflake entity asynchronously. \
The method consumes a [ISnowflakeDescribable](./pyflake_client/models/describables/snowflake_describable_interface.py).

The simplest example of a descibable is the [database]() describable. \
Note the implementation of `get_describe_statement`, `is_procedure` and `get_deserializer` method:

```python
@dataclass(frozen=True)
class Database(ISnowflakeDescribable):
    name: str

    def get_describe_statement(self) -> str:
        return f"SHOW DATABASES LIKE '{self.name}'".upper()

    def is_procedure(self) -> bool:
        return False

    @classmethod
    def get_deserializer(cls) -> Callable[[Dict[str, Any]], DatabaseEntity]:
        def deserialize(data: Dict[str, Any]) -> DatabaseEntity:
            return dacite.from_dict(DatabaseEntity, data, dacite.Config(type_hooks={int: lambda v: int(v)}))

        return deserialize
```

To clarify, the `is_procedure` is required as the return type from a procedure call is a string that must first be JSON-deserialized. \
This differs from the return type of a more regular `SHOW` or `SELECT` expression.

`describe_async` return a [AsyncDescribeJob](./pyflake_client//async_describe_job.py) which then must have one of the following functions invoked:

**wait():**

```python
describe_job = flake.describe_async(Database(name="MYDATABASE"))
describe_job.wait()
```
This will block until the job is completed.

**deserialize_one():**

By providing the class you want to deserialize your response into, and a deserializer for how to parse the response, you can deserialize_one()

```python
describe_job = flake.describe_async(Database(name="MYDATABASE"))
database_info = describe_job.deserialize_one(DatabaseEntity, custom_deserializer_func)
```
This will block until the job is completed.

**deserialize_many():**

By providing the class you want to deserialize your response into, and a deserializer for how to parse the response, you can deserialize_many()

```python
describe_job = flake.describe_async(Database(name="MYDATABASE"))
databases_info = describe_job.deserialize_many(DatabaseEntity, custom_deserializer_func)
```
This will block until the job is completed.

**is_done():**

```python
describe_job = flake.describe_async(Database(name="MYDATABASE"))
if describe_job.is_done():
    print("Done")
else:
    print("Not done")
```

**cancel():**

```python
describe_job = flake.describe_async(Database(name="MYDATABASE"))
describe_job.cancel()
```
This will block until the cancellation of the job is completed.

Note: Ensure that you replace `custom_deserializer_func` with the appropriate deserializer function if you're using this as a real example.


### Create, Read, Update and Delete (CRUD)

In order to insert, update and delete records you will use the [execute_async](#execute_async) method as described above. \
This can also be used to execute more complex upsert queries (merge into) as per [these](./pyflake_client/tests/test_mergeable.py) tests.


In order to read data and deserialize your response you can use either the `fetch_one` or `fetch_many` method on the execute_async return object, use [describe_async](#describe_async) with a combination of the [queryable](./pyflake_client/models/describables/queryable.py)

No matter which approach you chose, both methods must be provided with a custom deserializer method, instructing the method on how to deserialize your response.

For more inspiration/documentation on how to achieve the deserialization above, please see the deserialization lambdas that are use in [these](./pyflake_client/tests/test_callable_anonymous_procedure.py) tests 