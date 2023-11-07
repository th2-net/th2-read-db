# th2-read-db 0.3.4

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
  initQueryId: current_state
  updateQueryId: updates
  useColumns:
  - id
  interval: 1000
publication:
  queueSize: 1000
  maxDelayMillis: 1000
  maxBatchSize: 100
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
+ initQueryId - the id of the query that should be used to retrieve the current state of the database.
  NOTE: this parameter is used to initialize state and read-db doesn't publish retrieved messages to MQ router.
+ initParameters - the parameters that should be used in the init query. Also, The task uses these parameters to configure the first `updateQuery` execution if `initQuery` parameter is not specified
+ updateQueryId - the id of the query that should be used to pull updates from the database
+ useColumns - the set of columns that should be used in the update query (the last value from init query and from pull query)
+ updateParameters - the list of parameters that should be used in the update query
+ interval - the interval in millis to pull updates


# Interaction

You can interact with read-db via gRPC. It supports executing direct queries and submitting pull tasks.

# Publication

The read-db publishes all extracted data to MQ as raw messages in CSV format. The alias matches the **data source id**.
Message might contain property `th2.csv.override_message_type` with value that should be used as message type for the row message

# CR example

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: read-db
spec:
  image-name: ghcr.io/th2-net/th2-read-db
  image-version: 0.0.1
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
          name:
            - Ivan
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
    useTransport: true
  pins:
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

## Changes

### 0.3.4

#### Changed:

+ `initQuery` parameter in pull task is optional 

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