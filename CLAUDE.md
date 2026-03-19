# @prsm/queue

redis-backed distributed task queue with grouped concurrency, retries, and rate limiting.

## structure

```
src/
  index.js    - default export
  queue.js    - Queue class
tests/
  queue.test.js
```

## dev

```
make up        # start redis via docker compose
make test      # run tests
make down      # stop redis
```

redis must be running on localhost:6379 for tests.

## key decisions

- plain javascript, ESM, no build step
- single file implementation
- uses `redis` npm package (node-redis), not ioredis
- `@prsm/ms` for parsing duration strings ("100ms", "5s", "1m")
- types generated from JSDoc via `make types`
- cleanup timer is unref'd so it won't keep the process alive
- pushAndWait is distributed via redis pub/sub (queue:result:<uuid>)
- group workers are discovered cross-instance via redis pub/sub (queue:group:notify) and key scan at startup

## testing

tests use vitest. each test flushes redis in beforeEach. sequential execution.

## publishing

```
npm publish --access public
```

prepublishOnly generates types automatically.
