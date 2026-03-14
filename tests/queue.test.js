import { describe, it, expect, beforeEach, afterEach } from "vitest"
import Queue from "../src/index.js"
import { createClient } from "redis"

function waitForEvent(emitter, event, timeout = 5000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for "${event}" event`)), timeout)
    emitter.once(event, (data) => { clearTimeout(timer); resolve(data) })
  })
}

function collectEvents(emitter, event) {
  const events = []
  emitter.on(event, (data) => events.push(data))
  return events
}

describe("Queue", () => {
  let queue
  let redis

  beforeEach(async () => {
    redis = createClient()
    await redis.connect()
    await redis.flushAll()
  })

  afterEach(async () => {
    if (queue) await queue.close()
    if (redis) await redis.quit()
  })

  describe("basic functionality", () => {
    it("should push a task and emit new event", async () => {
      queue = new Queue()
      await queue.ready()

      const newPromise = waitForEvent(queue, "new")
      const uuid = await queue.push({ message: "test" })
      const { task } = await newPromise

      expect(uuid).toBeDefined()
      expect(typeof uuid).toBe("string")
      expect(task.uuid).toBe(uuid)
      expect(task.payload).toEqual({ message: "test" })
      expect(task.createdAt).toBeDefined()
      expect(task.attempts).toBe(0)
    })

    it("should process tasks with handler and emit complete with result", async () => {
      queue = new Queue({ concurrency: 1 })
      queue.process(async (payload) => ({ echo: payload.message }))
      await queue.ready()

      const completePromise = waitForEvent(queue, "complete")
      await queue.push({ message: "hello" })
      const { task, result } = await completePromise

      expect(task.payload).toEqual({ message: "hello" })
      expect(task.attempts).toBe(1)
      expect(result).toEqual({ echo: "hello" })
    })

    it("should complete without result when no handler set", async () => {
      queue = new Queue({ concurrency: 1 })
      await queue.ready()

      const completePromise = waitForEvent(queue, "complete")
      await queue.push({ message: "test" })
      const { task, result } = await completePromise

      expect(task).toBeDefined()
      expect(result).toBeUndefined()
    })
  })

  describe("retry logic", () => {
    it("should retry failed tasks up to maxRetries", async () => {
      let attempts = 0
      queue = new Queue({ concurrency: 1, maxRetries: 3 })
      queue.process(async () => { attempts++; throw new Error("simulated failure") })

      const retries = collectEvents(queue, "retry")
      const failedPromise = waitForEvent(queue, "failed")

      await queue.ready()
      await queue.push({ message: "retry test" })
      const { task } = await failedPromise

      expect(attempts).toBe(3)
      expect(retries.length).toBe(2)
      expect(retries[0].attempt).toBe(1)
      expect(retries[1].attempt).toBe(2)
      expect(task.attempts).toBe(3)
    })

    it("should succeed on retry if handler succeeds", async () => {
      let attempts = 0
      queue = new Queue({ concurrency: 1, maxRetries: 3 })
      queue.process(async () => { attempts++; if (attempts < 2) throw new Error("fail first time"); return "success" })

      const completePromise = waitForEvent(queue, "complete")
      await queue.ready()
      await queue.push({ message: "eventual success" })
      const { result } = await completePromise

      expect(attempts).toBe(2)
      expect(result).toBe("success")
    })

    it("should respect custom maxRetries", async () => {
      let attempts = 0
      queue = new Queue({ concurrency: 1, maxRetries: 5 })
      queue.process(async () => { attempts++; throw new Error("always fails") })

      const failedPromise = waitForEvent(queue, "failed")
      await queue.ready()
      await queue.push({ message: "max retry test" })
      await failedPromise

      expect(attempts).toBe(5)
    })
  })

  describe("grouped queues", () => {
    it("should create grouped tasks", async () => {
      queue = new Queue()
      await queue.ready()

      const newPromise = waitForEvent(queue, "new")
      const uuid = await queue.group("test-group").push({ message: "grouped test" })
      const { task } = await newPromise

      expect(uuid).toBeDefined()
      expect(task.groupKey).toBe("test-group")
      expect(task.payload).toEqual({ message: "grouped test" })
      expect(task.attempts).toBe(0)
    })

    it("should process grouped tasks with handler", async () => {
      queue = new Queue({ groups: { concurrency: 1 }, cleanupInterval: 0 })

      const results = []
      let completeCount = 0
      queue.process(async (payload) => { results.push(payload.id); return payload.id })

      const bothDone = new Promise((resolve) => {
        queue.on("complete", () => { completeCount++; if (completeCount === 2) resolve() })
      })

      await queue.ready()
      await queue.group("tenant-1").push({ id: "a" })
      await queue.group("tenant-1").push({ id: "b" })
      await bothDone

      expect(results).toContain("a")
      expect(results).toContain("b")
    })

    it("should retry grouped tasks", async () => {
      let attempts = 0
      queue = new Queue({ groups: { concurrency: 1, maxRetries: 2 }, cleanupInterval: 0 })
      queue.process(async () => { attempts++; throw new Error("group task fails") })

      const failedPromise = waitForEvent(queue, "failed")
      await queue.ready()
      await queue.group("retry-group").push({ message: "test" })
      const { task } = await failedPromise

      expect(attempts).toBe(2)
      expect(task.groupKey).toBe("retry-group")
    })

    it("should process different groups independently", async () => {
      queue = new Queue({ groups: { concurrency: 1 }, cleanupInterval: 0 })

      let running = 0
      let maxRunning = 0
      queue.process(async () => {
        running++
        maxRunning = Math.max(maxRunning, running)
        await new Promise((resolve) => setTimeout(resolve, 50))
        running--
        return "done"
      })

      let completeCount = 0
      const allDone = new Promise((resolve) => {
        queue.on("complete", () => { completeCount++; if (completeCount === 2) resolve() })
      })

      await queue.ready()
      await queue.group("g1").push({ id: 1 })
      await queue.group("g2").push({ id: 2 })
      await allDone

      expect(maxRunning).toBe(2)
      expect(completeCount).toBe(2)
    })
  })

  describe("timeout", () => {
    it("should emit failed on timeout after retries exhausted", async () => {
      queue = new Queue({ timeout: 50, concurrency: 1, maxRetries: 2 })
      queue.process(async () => { await new Promise((resolve) => setTimeout(resolve, 200)); return "should timeout" })

      const retries = collectEvents(queue, "retry")
      const failedPromise = waitForEvent(queue, "failed")

      await queue.ready()
      await queue.push({ message: "timeout test" })
      const { task, error } = await failedPromise

      expect(task).toBeDefined()
      expect(error.message).toBe("Task timeout")
      expect(retries.length).toBe(1)
    })
  })

  describe("concurrency", () => {
    it("should process tasks in parallel with concurrency > 1", async () => {
      queue = new Queue({ concurrency: 3 })

      let running = 0
      let maxRunning = 0
      queue.process(async () => {
        running++
        maxRunning = Math.max(maxRunning, running)
        await new Promise((resolve) => setTimeout(resolve, 50))
        running--
        return "done"
      })

      let completeCount = 0
      const allDone = new Promise((resolve) => {
        queue.on("complete", () => { completeCount++; if (completeCount === 3) resolve() })
      })

      await queue.ready()
      await queue.push({ id: 1 })
      await queue.push({ id: 2 })
      await queue.push({ id: 3 })
      await allDone

      expect(maxRunning).toBe(3)
    })

    it("should limit parallel processing to concurrency value", async () => {
      queue = new Queue({ concurrency: 2 })

      let running = 0
      let maxRunning = 0
      queue.process(async () => {
        running++
        maxRunning = Math.max(maxRunning, running)
        await new Promise((resolve) => setTimeout(resolve, 50))
        running--
        return "done"
      })

      let completeCount = 0
      const allDone = new Promise((resolve) => {
        queue.on("complete", () => { completeCount++; if (completeCount === 4) resolve() })
      })

      await queue.ready()
      await queue.push({ id: 1 })
      await queue.push({ id: 2 })
      await queue.push({ id: 3 })
      await queue.push({ id: 4 })
      await allDone

      expect(maxRunning).toBe(2)
      expect(completeCount).toBe(4)
    })
  })

  describe("drain", () => {
    it("should emit drain after tasks complete", async () => {
      queue = new Queue({ concurrency: 1 })
      queue.process(async () => "done")
      await queue.ready()

      const drainPromise = waitForEvent(queue, "drain")
      await queue.push({ message: "test" })
      await drainPromise

      expect(queue.inFlight).toBe(0)
    })

    it("should emit drain after retries exhaust and task fails", async () => {
      queue = new Queue({ concurrency: 1, maxRetries: 2 })
      queue.process(async () => { throw new Error("always fails") })
      await queue.ready()

      const drainPromise = waitForEvent(queue, "drain")
      await queue.push({ message: "will fail" })
      await drainPromise

      expect(queue.inFlight).toBe(0)
    })
  })
})
