# gRPC for read-db (0.0.5)

The read-db provides you with gRPC interface for interacting with database.
You can:

+ request data synchronously - `Execute` method
+ submit pulling requests and stop them - `StartPulling` and `StopPulling` methods

# Release notes:

## 0.0.5
+ added DbPullRequest.reset_state_parameters field

## 0.0.4
+ added DbPullRequest.start_from_last_read_row field