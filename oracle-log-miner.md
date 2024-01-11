# Pull Oracle redo log

The most crucial structure for recovery operations is the [redo log](https://docs.oracle.com/cd/A97385_01/server.920/a96521/onlineredo.htm)
`Redo logs` contain information for recovery queries executed in site identifier (`SID`).
Also, an administrator can enable [Supplemental Logging](https://docs.oracle.com/cd/A97385_01/server.920/a96521/logminer.htm#25840) to store reconstructed SQL statements. 
After that an administrator can run [Log Miner](https://docs.oracle.com/cd/A97385_01/server.920/a96521/logminer.htm) 
and read reconstructed SQL statements from a special dynamic view `V$LOGMNR_CONTENTS`

## Create administrator role to use `Log Miner`

Oracle user should have special permissions to work with `Log Miner`.
All instructions below should be executed under `sys` user.

```roomsql
CREATE USER <user> IDENTIFIED BY <pass> DEFAULT TABLESPACE users QUOTA UNLIMITED ON users ACCOUNT UNLOCK;

GRANT DBA to <user>;
GRANT CREATE SESSION TO <user>;
GRANT EXECUTE_CATALOG_ROLE TO <user>;
GRANT EXECUTE ON DBMS_LOGMNR TO <user>;
GRANT SELECT ON V_$DATABASE TO <user>;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO <user>;
GRANT SELECT ON V_$ARCHIVED_LOG TO <user>;
GRANT SELECT ON V_$LOG TO <user>;
GRANT SELECT ON V_$LOGFILE TO <user>;
```

All instructions below should be executed under created user.

## `Supplemental Logging` management

Oracle begins storing additional information into `Redo logs` after enabling `Supplemental Logging`.
It means that `Supplemental Logging` should be enabled before pulling data.

### Check
```roomsql
SELECT SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE;
```
```roomsql
SUPPLEME
--------
YES
```

### Enable

Enable `Supplemental Logging` for `SID`
```roomsql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
```

### Disable
```roomsql
ALTER DATABASE DROP SUPPLEMENTAL LOG DATA;
```

## `Log miner` management

### Show redo log files
```roomsql
SELECT distinct member LOGFILENAME FROM V$LOGFILE;
```
```roomsql
LOGFILENAME
--------------------------------------------------------------------------------
/opt/oracle/oradata/XE/redo01.log
/opt/oracle/oradata/XE/redo02.log
/opt/oracle/oradata/XE/redo03.log
```

### Configure `Log miner`
```roomsql
EXECUTE DBMS_LOGMNR.ADD_LOGFILE (LOGFILENAME => '/opt/oracle/oradata/XE/redo01.log', OPTIONS => DBMS_LOGMNR.NEW);
EXECUTE DBMS_LOGMNR.ADD_LOGFILE (LOGFILENAME => '/opt/oracle/oradata/XE/redo02.log', OPTIONS => DBMS_LOGMNR.ADDFILE);
EXECUTE DBMS_LOGMNR.ADD_LOGFILE (LOGFILENAME => '/opt/oracle/oradata/XE/redo03.log', OPTIONS => DBMS_LOGMNR.ADDFILE);
```
```roomsql
PL/SQL procedure successfully completed.
```

### Start `Log miner`
```roomsql
EXECUTE DBMS_LOGMNR.START_LOGMNR (OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY);
```
```roomsql
PL/SQL procedure successfully completed.
```

### Select SQL statement

Kindly note that there is a limit in 4000 character per SQL_REDO column.
If the SQL_REDO value to insert exceeds this limit it will be split into several records
with same SCN, RS_ID and SSN columns.
You can read more information by the [following link](https://docs.oracle.com/en/database/oracle/oracle-database/23/refrn/V-LOGMNR_CONTENTS.html).
Also, `0x00` (null) character are replaces with ` ` (space) to avoid issues with XMLAGG function.

```roomsql
SELECT SCN, RS_ID, SSN,
MAX(TIMESTAMP) AS TIMESTAMP, 
MAX(TABLE_NAME) AS TABLE_NAME, 
MAX(ROW_ID) AS ROW_ID, 
MAX(OPERATION) AS OPERATION, 
XMLAGG(XMLPARSE(CONTENT REPLACE(SQL_REDO, CHR(0), ' ')) ORDER BY SCN, RS_ID, SSN, ROWNUM).GETCLOBVAL() AS SQL_REDO
FROM V$LOGMNR_CONTENTS
GROUP BY SCN, RS_ID, SSN;
```
```roomsql
SCN     TIMESTAMP            OPERATION TABLE_NAME ROW_ID             SQL_REDO
------- -------------------- --------- ---------- ------------------ ------------
...
3627247 06-Dec-2023 08:54:31 INSERT    EMPLOYEE   AAASt5AAHAAAAFcAAA insert into "<user>"."EMPLOYEE"("ID","NAME","TITLE","SALARY","BONUS_STRUCTURE","TIME_OFF","SICK_TIME", ...
...
3627285 06-Dec-2023 08:54:31 UPDATE    EMPLOYEE   AAASt5AAHAAAAFcAAA update "<user>"."EMPLOYEE" set "SAVINGS" = '10' where "SAVINGS" = '1' and ROWID = 'AAASt5AAHAAAAFcAAA';
...
3627294	06-Dec-2023 08:54:31 DELETE    EMPLOYEE   AAASt5AAHAAAAFcAAA delete from "<user>"."EMPLOYEE" where "ID" = '1' and "NAME" = 'Chris Montgomery                     ', ...
...
```

The whole list of columns is described in the [V$LOGMNR_CONTENTS](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/V-LOGMNR_CONTENTS.html#GUID-B9196942-07BF-4935-B603-FA875064F5C3)

### Stop `Log miner`
```roomsql
EXECUTE DBMS_LOGMNR.END_LOGMNR;
```
```roomsql
PL/SQL procedure successfully completed.
```

## th2-read-db configuration
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
      db:
        url: "jdbc:oracle:thin:@$<host>:1521:XE"
        username: <user>
        password: <password>
    queries:
      add-logfile-1:
        query: "{CALL DBMS_LOGMNR.ADD_LOGFILE (LOGFILENAME => '/opt/oracle/oradata/XE/redo01.log', OPTIONS => DBMS_LOGMNR.NEW)}"
      add-logfile-2:
        query: "{CALL DBMS_LOGMNR.ADD_LOGFILE (LOGFILENAME => '/opt/oracle/oradata/XE/redo02.log', OPTIONS => DBMS_LOGMNR.ADDFILE)}"
      add-logfile-3:
        query: "{CALL DBMS_LOGMNR.ADD_LOGFILE (LOGFILENAME => '/opt/oracle/oradata/XE/redo03.log', OPTIONS => DBMS_LOGMNR.ADDFILE)}"
      start-log-miner:
        query: "{CALL DBMS_LOGMNR.START_LOGMNR (OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY)}"
      end-log-miner:
        query: "{CALL DBMS_LOGMNR.END_LOGMNR}"
      update-logmnr-contents:
        query: >
          SELECT SCN, RS_ID, SSN,
          MAX(TIMESTAMP) AS TIMESTAMP,
          MAX(TABLE_NAME) AS TABLE_NAME,
          MAX(ROW_ID) AS ROW_ID,
          MAX(OPERATION) AS OPERATION,
          XMLAGG(XMLPARSE(CONTENT REPLACE(SQL_REDO, CHR(0), ' ')) ORDER BY SCN, RS_ID, SSN, ROWNUM).GETCLOBVAL() AS SQL_REDO
          FROM V$LOGMNR_CONTENTS
          WHERE TABLE_NAME = 'YOUR_TABLE' AND SCN > ${SCN:integer} 
          AND OPERATION in ('INSERT', 'DELETE', 'UPDATE')
          GROUP BY SCN, RS_ID, SSN
    startupTasks:
      - type: pull
        dataSource: db
        startFromLastReadRow: true
        initParameters:
          SCN:
            - "-1"
        beforeUpdateQueryIds:
          - add-logfile-1
          - add-logfile-2
          - add-logfile-3
          - start-log-miner
        updateQueryId: update-logmnr-contents
        afterUpdateQueryIds:
          - end-log-miner
        useColumns:
          - SCN
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
