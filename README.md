This is a partial implementation for a Redis client for the CodeCrafters
["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis)
in rust.

This implementation uses Tokio for asynchronous handling, but, importantly,
follows the actual redis implementation and is purely single threaded to
avoid using any locking.
