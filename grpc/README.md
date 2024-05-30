# gRPC for read-db (0.0.10)

The read-db provides you with gRPC interface for interacting with database.
You can:

+ request data synchronously - `Execute` method. This method returns rows as stream.
+ run data loading synchronously - `Load` method. This method returns aggregated execution report.
+ submit pulling requests and stop them - `StartPulling` and `StopPulling` methods

# Release notes:

## 0.0.10
+ updated th2 gradle plugin `0.0.8`

## 0.0.9
+ updated grpc-common: `4.5.0-dev`

## 0.0.8
+ added `Load` method

## 0.0.7
+ added execution_id to the QueryResponse
+ added parent_event_id to the QueryRequest

## 0.0.6
+ added before_init_query_ids, after_init_query_ids, before_update_query_ids, after_update_query_ids to the DbPullRequest
+ added before_query_ids, after_query_ids to the QueryRequest

## 0.0.5
+ added DbPullRequest.reset_state_parameters field

## 0.0.4
+ added DbPullRequest.start_from_last_read_row field