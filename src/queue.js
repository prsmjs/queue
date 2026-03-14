import { createClient } from "redis"
import { EventEmitter } from "events"
import { randomUUID } from "crypto"
import ms from "@prsm/ms"

/**
 * @typedef {Object} QueueOptions
 * @property {number} [concurrency] - worker count (default 1)
 * @property {number|string} [delay] - pause between tasks, ms or string like "100ms" (default 0)
 * @property {number|string} [timeout] - max task duration, ms or string like "30s" (default 0, no limit)
 * @property {number} [maxRetries] - attempts before failing (default 3)
 * @property {{concurrency?: number, delay?: number|string, timeout?: number|string, maxRetries?: number}} [groups] - overrides for grouped queues
 * @property {{url?: string, host?: string, port?: number, password?: string}} [redisOptions]
 * @property {number} [cleanupInterval] - ms between empty group cleanup (default 30000, 0 to disable)
 */

/**
 * @typedef {Object} Task
 * @property {string} uuid
 * @property {any} payload
 * @property {number} createdAt
 * @property {string} [groupKey]
 * @property {number} attempts
 */

/**
 * @callback TaskHandler
 * @param {any} payload
 * @param {Task} task
 * @returns {Promise<any>|any}
 */

export default class Queue extends EventEmitter {
  /** @param {QueueOptions} [options] */
  constructor(options = {}) {
    super()

    this._options = {
      concurrency: options.concurrency ?? 1,
      delay: ms(options.delay ?? 0),
      timeout: ms(options.timeout ?? 0),
      maxRetries: options.maxRetries ?? 3,
      groups: {
        concurrency: options.groups?.concurrency ?? options.concurrency ?? 1,
        delay: ms(options.groups?.delay ?? options.delay ?? 0),
        timeout: ms(options.groups?.timeout ?? options.timeout ?? 0),
        maxRetries: options.groups?.maxRetries ?? options.maxRetries ?? 3,
      },
      redisOptions: options.redisOptions ?? {},
      cleanupInterval: options.cleanupInterval ?? 30000,
    }

    this._handler = null
    this._workers = new Map()
    this._groupWorkers = new Map()
    this._workerClients = []
    this._cleanupTimer = null
    this._inFlight = 0
    this._totalSettled = 0

    this._redis = createClient(this._options.redisOptions)
    this._redis.on("error", () => {})
    this._readyPromise = this._initialize()
  }

  /** @returns {Promise<void>} */
  ready() {
    return this._readyPromise
  }

  /** @returns {number} */
  get inFlight() {
    return this._inFlight
  }

  /** @param {TaskHandler} handler */
  process(handler) {
    this._handler = handler
  }

  /**
   * @param {any} payload
   * @returns {Promise<string>}
   */
  async push(payload) {
    const task = { uuid: randomUUID(), payload, createdAt: Date.now(), attempts: 0 }
    await this._redis.lPush("queue:tasks", JSON.stringify(task))
    this.emit("new", { task })
    return task.uuid
  }

  /**
   * @param {string} key
   * @returns {{ push: (payload: any) => Promise<string> }}
   */
  group(key) {
    return {
      push: async (payload) => {
        const task = { uuid: randomUUID(), payload, createdAt: Date.now(), groupKey: key, attempts: 0 }
        await this._redis.lPush(`queue:groups:${key}`, JSON.stringify(task))
        this.emit("new", { task })
        if (!this._groupWorkers.has(key)) {
          this._groupWorkers.set(key, new Map())
          await this._startGroupWorkers(key)
        }
        return task.uuid
      },
    }
  }

  /** @returns {Promise<void>} */
  async close() {
    await this._readyPromise.catch(() => {})
    if (this._cleanupTimer) clearInterval(this._cleanupTimer)
    this._workers.clear()
    for (const groupWorkers of this._groupWorkers.values()) groupWorkers.clear()
    this._groupWorkers.clear()
    for (const client of this._workerClients) {
      if (client.isOpen) await client.disconnect()
    }
    this._workerClients = []
    if (this._redis.isOpen) await this._redis.quit()
  }

  async _initialize() {
    await this._redis.connect()
    await this._startWorkers()
    if (this._options.cleanupInterval > 0) {
      this._cleanupTimer = setInterval(() => this._periodicCleanup(), this._options.cleanupInterval)
      this._cleanupTimer.unref()
    }
  }

  async _createWorkerClient() {
    const client = this._redis.duplicate()
    client.on("error", () => {})
    await client.connect()
    this._workerClients.push(client)
    return client
  }

  async _startWorkers() {
    const ready = []
    for (let i = 0; i < this._options.concurrency; i++) {
      ready.push(this._startWorker(`worker-${i}`))
    }
    await Promise.all(ready)
  }

  async _startGroupWorkers(groupKey) {
    const groupWorkers = this._groupWorkers.get(groupKey)
    const ready = []
    for (let i = 0; i < this._options.groups.concurrency; i++) {
      const workerId = `group-${groupKey}-worker-${i}`
      groupWorkers.set(workerId, true)
      ready.push(this._startGroupWorker(workerId, groupKey))
    }
    await Promise.all(ready)
  }

  async _startWorker(workerId) {
    this._workers.set(workerId, true)
    const client = await this._createWorkerClient()
    this._runWorkerLoop(workerId, client, "queue:tasks", this._workers, (task) => this._processTask(task))
  }

  async _startGroupWorker(workerId, groupKey) {
    const groupWorkers = this._groupWorkers.get(groupKey)
    const client = await this._createWorkerClient()
    this._runWorkerLoop(workerId, client, `queue:groups:${groupKey}`, groupWorkers, (task) => this._processGroupTask(task))
  }

  async _runWorkerLoop(workerId, client, key, activeMap, processFn) {
    const isGrouped = key.startsWith("queue:groups:")
    const delay = isGrouped ? this._options.groups.delay : this._options.delay

    while (activeMap.get(workerId)) {
      try {
        if (!client.isOpen) break
        const taskData = await client.brPop(key, 1)
        if (taskData) {
          const task = JSON.parse(taskData.element)
          this._inFlight++
          await processFn(task)
        }
        if (delay > 0) await new Promise((resolve) => setTimeout(resolve, delay))
      } catch (err) {
        if (err.message?.includes("closed") || err.message?.includes("ClientClosedError")) break
      }
    }
  }

  async _processTask(task) {
    task.attempts++
    try {
      if (!this._handler) {
        this.emit("complete", { task, result: undefined })
        this._settle()
        return
      }
      const timeoutPromise = this._options.timeout > 0
        ? new Promise((_, reject) => setTimeout(() => reject(new Error("Task timeout")), this._options.timeout))
        : null
      const workPromise = Promise.resolve(this._handler(task.payload, task))
      const result = timeoutPromise ? await Promise.race([workPromise, timeoutPromise]) : await workPromise
      this.emit("complete", { task, result })
      this._settle()
    } catch (error) {
      if (task.attempts < this._options.maxRetries) {
        this.emit("retry", { task, error, attempt: task.attempts })
        this._inFlight--
        await this._redis.lPush("queue:tasks", JSON.stringify(task))
      } else {
        this.emit("failed", { task, error })
        this._settle()
      }
    }
  }

  async _processGroupTask(task) {
    task.attempts++
    try {
      if (!this._handler) {
        this.emit("complete", { task, result: undefined })
        this._settle()
        return
      }
      const timeoutPromise = this._options.groups.timeout > 0
        ? new Promise((_, reject) => setTimeout(() => reject(new Error("Task timeout")), this._options.groups.timeout))
        : null
      const workPromise = Promise.resolve(this._handler(task.payload, task))
      const result = timeoutPromise ? await Promise.race([workPromise, timeoutPromise]) : await workPromise
      this.emit("complete", { task, result })
      this._settle()
    } catch (error) {
      if (task.attempts < this._options.groups.maxRetries) {
        this.emit("retry", { task, error, attempt: task.attempts })
        this._inFlight--
        await this._redis.lPush(`queue:groups:${task.groupKey}`, JSON.stringify(task))
      } else {
        this.emit("failed", { task, error })
        this._settle()
      }
    }
  }

  _settle() {
    this._inFlight--
    this._totalSettled++
    if (this._inFlight === 0 && this._totalSettled > 0) this.emit("drain")
  }

  async _periodicCleanup() {
    try {
      if (!this._redis.isOpen) return
      const groupKeys = Array.from(this._groupWorkers.keys())
      for (const groupKey of groupKeys) {
        const length = await this._redis.lLen(`queue:groups:${groupKey}`)
        if (length === 0) {
          const keyExists = await this._redis.exists(`queue:groups:${groupKey}`)
          if (keyExists) await this._redis.del(`queue:groups:${groupKey}`)
          const groupWorkers = this._groupWorkers.get(groupKey)
          if (groupWorkers) {
            groupWorkers.clear()
            this._groupWorkers.delete(groupKey)
          }
        }
      }
    } catch {}
  }
}
