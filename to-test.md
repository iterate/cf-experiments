# Stuff to test

This is also stuff that we should challenge agents to break 

- We can have arbitraryly deep nesting of processors - no issues with workers subrequest limits 
- It should NEVER be possible for a stream processor to receive events out of offset order
- when the request context that caused an append() no longer exists, we still need the delivery of events to subscribers to continue - possibly this means we need to always open outbound websockets from an alarm
