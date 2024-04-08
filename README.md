# th2-read-db 0.9.1

The read-db is a component for extracting data from databases using JDBC technology. If database has JDBC driver the read can work with the database

# Configuration

The read-db should be configured with predefined data sources and queries it can connect and execute.
Here is an example of such configuration and parameters explanation:

```yaml
dataSources:
  persons:
    url: "jdbc:mysql://192.168.0.1:3306/people"
    username: user
    password: pwd
    properties:
      prop1: value1
queries:
  all:
    query: "SELECT * FROM person WHERE birthday > ${birthday:date};"
    defaultParameters:
      birthday:
      - 1996-01-31
  current_state:
    query: "SELECT * FROM person ORDER BY id DESC LIMIT 1;"
  updates:
    query: "SELECT * FROM person WHERE id > ${id:integer};"
startupTasks:
- type: read
  dataSource: persons
  queryId: all
  parameters:
    name:
    - Ivan
- type: pull
  dataSource: persons
  startFromLastReadRow: false
  initQueryId: current_state
  updateQueryId: updates
  useColumns:
  - id
  interval: 1000
publication:
  queueSize: 1000
  maxDelayMillis: 1000
  maxBatchSize: 100
eventPublication:
  maxBatchSizeInItems: 100
  maxFlushTime: 1000
```

## Parameters

### dataSources

The list of data sources where the read-db can connect.

+ url - the url to database. The format depends on the JDBC driver that should be used to connect to this database.
+ username - the username that should be used when connecting to database. Skip if other types of authentication should be used
+ password - the password that should be used when connecting to database. Skip if other types of authentication should be used
+ parameters - the list of parameters with their values that must be used when connecting to database. They are specific for each database

### queries

The list of queries that can be executed by read-db.

+ query - the raw query in SQL that should be executed. 
  It might contain parameters in the following format: `${<name>[:<type>]}`.
  The **type** part can be omitted if the type is `varchar`.
  Examples: `${id:integer}`, `${registration_time:timestamp}`, `${first_name}`
  [Types](https://docs.oracle.com/javase/8/docs/api/java/sql/JDBCType.html): bit, tinyint, smallint, integer, bigint, float, real, double, numeric, decimal, char, varchar, longvarchar, date, time, timestamp, binary, varbinary, longvarbinary, null, other, java_object, distinct, struct, array, blob, clob, ref, datalink, boolean, rowid, nchar, nvarchar, longnvarchar, nclob, sqlxml, ref_cursor, time_with_timezone, timestamp_with_timezone
+ defaultParameters - the default values for parameters. They will be used if the parameter was not specified in the request
+ messageType - the message type that should be associated with this query.
  If it is set the read-db will set a property `th2.csv.override_message_type` with specified value

### startupTasks

The list of task that should be executed on the start of read-db.
There are two types of tasks: **read** and **pull**.
The type is specified in `type` field.

#### read

The read tasks tries to read all data from the specified data source using specified query and parameters.

+ dataSource - the id of the source that should be used
+ queryId - the id of the query that should be used
+ parameters - the list of parameters that should be used in the query

#### pull

Pulls updates from the specified data source using the specified queries.

+ dataSource - the id of the source that should be used
+ startFromLastReadRow - task tries to load previous state via `data-provider` if this option is `true`
+ resetStateParameters - optional parameters to scheduled reset internal state and re-init task. 
  + afterDate - optional parameter with date time in [ISO_INSTANT](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#ISO_INSTANT) format, for example: `"2023-11-14T12:12:34.567890123Z"`
    The option is used to set single reset at date.
  + afterTime - optional parameter with time in [ISO_LOCAL_TIME](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#ISO_LOCAL_TIME) format, for example: `"12:12:34.567890123"`
    The time value must be specified in the UTC zone.
    The option is used to set every day reset at time.
+ initQueryId - the id of the query that should be used to retrieve the current state of the database.
  NOTE: this parameter is used to initialize state and read-db doesn't publish retrieved messages to MQ router.
+ initParameters - the parameters that should be used in the init query. Also, The task uses these parameters to configure the first `updateQuery` execution if `initQuery` parameter is not specified
+ updateQueryId - the id of the query that should be used to pull updates from the database
+ useColumns - the set of columns that should be used in the update query (the last value from init query and from pull query)
+ updateParameters - the list of parameters that should be used in the update query
+ interval - the interval in millis to pull updates

##### behaviour

This type of task work by the algorithm:

1) Initialize parameters for the first `updateQuery`
   * task tris to load the last message with `th2.pull_task.update_hash` property published to Cradle if startFromLastReadRow is `true`.
     the time boundary for message loading is the nearest reset time calculated by `resetStateParameters` option if set, otherwise the execution time minus one day<br>
     **NOTE**: if read-db isn't connected to a data-provider [Go to gRPC client configuration](#client), the task failures.
   * if `startFromLastReadRow` is `false` or no one message hasn't been published into Cradle by related session alias, task tries to execute init query.
   * if init query is `null`, task uses `initProperties` to initialize property for the first `updateQuery` run.<br>
   NOTE: if `initProperties` doesn't defined, the first `updateQuery` is run with `NULL` value for all used parameters
2) task periodically executes `updateQuery` with parameters specified in `updateParameters` option and parameters initialised on the previous step.

Pull task send all messages loaded from database via pins with `transport-group`, `publish` attributes for the transport mode and `raw`, `publish` for protobuf mode.
Each message has `th2.pull_task.update_hash` property calculated by source and query configurations.

# Interaction

You can interact with read-db via gRPC. It supports executing direct queries and submitting pull tasks.

# Publication

The read-db publishes all extracted data to MQ as raw messages in CSV format. The alias matches the **data source id**.
Message might contain properties
* `th2.csv.override_message_type` with value that should be used as message type for the row message
* `th2.read-db.execute.uid` with unique identifier of query execution
* `th2.pull_task.update_hash` with hash of source and query configuration used pull query execution 

# gRPC

## Client

Pull task tries to load the last message published to Cradle instead of initialise from the start 
if you connect read-db to a data-provider using `com.exactpro.th2.dataprovider.lw.grpc.DataProviderService`. 

## Server

### Execute method

User can trigger a query execution on a data source using this method. the method includes the activities:
* generation of growing unique id.
* query execution.
* publication results of the query execution to MQ where each message has `th2.read-db.execute.uid` property with the unique id
* publication event with data source, query, request parameters and the unique id. 
  Start/End even times correspond to the beginning/ending the query execution.
  Body example:
  ```json
  [
    {
      "dataSource": {
        "url":"jdbc url for data base connection",
        "username":"user name"
      },
      "query": {
        "query":"SQL query text"
      },
      "parameters": {
        "parameter": [
          "parameter value"
        ]
      },
      "executionId": 123
    }
  ]
  ```
  NOTE: the event hasn't got attached message because the query can produce a lot of rows.
* streaming results of the query execution with the unique id as gRPC response. 

# CR example
## infra 1
```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: read-db
spec:
  image-name: ghcr.io/th2-net/th2-read-db
  image-version: 0.7.0-dev
  type: th2-read
  custom-config:
    dataSources:
      persons:
        url: "jdbc:mysql://192.168.0.1:3306/people"
        username: user
        password: pwd
        properties:
          prop1: value1
    queries:
      all:
        query: "SELECT * FROM person WHERE birthday > ${birthday:date};"
        defaultParameters:
          birthday:
            - 1996-01-31
      current_state:
        query: "SELECT * FROM person ORDER BY id DESC LIMIT 1;"
      updates:
        query: "SELECT * FROM person WHERE id > ${id:integer};"
    startupTasks:
      - type: read
        dataSource: persons
        queryId: all
        parameters:
          birthday:
            - 1997-02-01
      - type: pull
        dataSource: persons
        initQueryId: current_state
        updateQueryId: updates
        useColumns:
          - id
        interval: 1000
    publication:
      queueSize: 1000
      maxDelayMillis: 1000
      maxBatchSize: 100
    eventPublication:
      maxBatchSizeInItems: 100
      maxFlushTime: 1000
    useTransport: true
  pins:
    - name: client
      connection-type: grpc-client
      service-class: com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
    - name: server
      connection-type: grpc-server
      service-classes:
        - com.exactpro.th2.read.db.grpc.ReadDbService
        - th2.read_db.ReadDbService
    - name: store
      connection-type: mq
      attributes: ['transport-group', 'publish', 'store']
  extended-settings:
    service:
      enabled: false
    envVariables:
      JAVA_TOOL_OPTIONS: "-XX:+ExitOnOutOfMemoryError"
    resources:
      limits:
        memory: 500Mi
        cpu: 600m
      requests:
        memory: 100Mi
        cpu: 50m
```

## infra 2
```yaml
apiVersion: th2.exactpro.com/v2
kind: Th2Box
metadata:
  name: read-db
spec:
  imageName: ghcr.io/th2-net/th2-read-db
  imageVersion: 0.7.0-dev
  type: th2-read
  customConfig:
    dataSources:
      persons:
        url: "jdbc:mysql://192.168.0.1:3306/people"
        username: user
        password: pwd
        properties:
          prop1: value1
    queries:
      all:
        query: "SELECT * FROM person WHERE birthday > ${birthday:date};"
        defaultParameters:
          birthday:
            - 1996-01-31
      current_state:
        query: "SELECT * FROM person ORDER BY id DESC LIMIT 1;"
      updates:
        query: "SELECT * FROM person WHERE id > ${id:integer};"
    startupTasks:
      - type: read
        dataSource: persons
        queryId: all
        parameters:
          birthday:
            - 1997-02-01
      - type: pull
        dataSource: persons
        startFromLastReadRow: false
        initQueryId: current_state
        updateQueryId: updates
        useColumns:
          - id
        interval: 1000
    publication:
      queueSize: 1000
      maxDelayMillis: 1000
      maxBatchSize: 100
    eventPublication:
      maxBatchSizeInItems: 100
      maxFlushTime: 1000
    useTransport: true
  pins:
    mq:
      publishers:
        - name: store
          attributes: ['transport-group', 'publish', 'store']
    grpc:
      client:
        - name: to_data_provider
          serviceClass: com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
          linkTo:
            - box: lw-data-provider
              pin: server
      server:
        - name: server
          serviceClasses:
            - com.exactpro.th2.read.db.grpc.ReadDbService
            - th2.read_db.ReadDbService
  extendedSettings:
    service:
      enabled: false
    envVariables:
      JAVA_TOOL_OPTIONS: "-XX:+ExitOnOutOfMemoryError"
    resources:
      limits:
        memory: 500Mi
        cpu: 600m
      requests:
        memory: 100Mi
        cpu: 50m
```

### Oracle redo logs
[How to configure th2-read-db to pull data from redo log](oracle-log-miner.md)

## Changes

### 0.9.1

+ fixed the pull continuation failure when column with oracle DATE type is used for update query. 

### 0.9.0

+ implemented gRPC backpressure for the `Execute` method
+ updated jdbc:
  + mysql-connector-j:`8.3.0`
  + ojdbc11:`23.3.0.23.09`
  + postgresql:`42.7.3`
+ updated: 
  + common:`5.10.0-dev`
  + grpc-common:`4.4.0-dev`
  + common-utils:`2.2.2-dev`
  + common-utils:`2.2.2-dev`

### 0.8.0

+ implemented the `Load` gRPC method.
+ fixed the catching java Error such as OutOfMemoryError problem
+ updated bom:`4.6.0`
  

### 0.7.0

#### Feature:

+ gRPC execute method generates unique id for each execution and puts it into related event and messages.

#### Fix:

+ gRPC Execute method doesn't respond rows with null values. gRPC server implementation skips columns with null value after fix.

### 0.6.0

#### Feature:

+ added beforeInitQueryIds, afterInitQueryIds, beforeUpdateQueryIds, afterUpdateQueryIds properties into config

### 0.5.0

#### Feature:

+ added the `reset state parameters` option to configure static or dynamic dates of reset 

#### Update:

+ grpc-read-db: `0.0.5`

### 0.4.0

#### Feature:

#### Changed:

+ pull task optionally loads the last message for initialisation from a data-provider via gRPC

#### Update:

+ common: `5.7.1-dev`
+ grpc-service-generator: `3.5.1`
+ grpc-read-db: `0.0.4`

### 0.3.4

#### Changed:

+ `initQuery` parameter in a pull task is made optional

### 0.3.3

### Fix:

+ read-db prints `byte array` as object hash code instead of converting to HEX string  

### 0.3.2

#### Changed:

+ remove redundant dependencies from gRPC

### 0.3.0

+ MSSQL support added

### 0.2.0

+ Added support for th2 transport protocol