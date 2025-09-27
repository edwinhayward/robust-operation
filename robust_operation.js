// robust-operation.js

/**
 * @file A self-contained, robust operation utility for handling asynchronous
 *       tasks with automatic retries, per-attempt timeouts, an overall deadline,
 *       jittered backoff, and AbortSignal composition.
 *       Internally uses a monotonic clock to avoid deadline drift from wall-clock changes.
 * @version 1.0.0
 */

/**
 * Custom error class for timeout conditions.
 * @extends Error
 */
export class TimeoutError extends Error {
  constructor(msg = 'Timeout') {
    super(msg);
    this.name = 'TimeoutError';
    this.code = 'ETIMEOUT';
    this.isTimeout = true;
  }
}

/**
 * Custom error class for integrity check failures.
 * @extends Error
 */
export class IntegrityError extends Error {
  constructor(msg = 'Integrity error') {
    super(msg);
    this.name = 'IntegrityError';
    this.code = 'EINTEGRITY';
    this.isIntegrityError = true;
  }
}

/**
 * Returns the value `v` if it is a finite number, otherwise returns the default `d`.
 * @param {*} v - The value to check.
 * @param {number} d - The default value to return if `v` is not a finite number.
 * @returns {number} The value `v` or the default `d`.
 */
function finiteOr(v, d) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

/**
 * Normalizes a timeout-like value:
 * - NaN or undefined -> defaultMs
 * - +Infinity -> +Infinity (explicitly means "no timeout")
 * - -Infinity -> 0
 * - finite -> clamped to >= 0
 * @param {*} v
 * @param {number} defaultMs
 * @returns {number}
 */
function timeoutOr(v, defaultMs) {
  const n = Number(v);
  if (Number.isNaN(n)) return defaultMs;
  if (n === Infinity) return Infinity;
  if (n === -Infinity) return 0;
  return Math.max(0, n);
}

/**
 * Returns a monotonic millisecond timestamp suitable for measuring durations.
 * Prefers performance.now(), falls back to process.hrtime.bigint(), then Date.now().
 * @returns {number}
 */
function monotonicNowMs() {
  try {
    if (typeof globalThis.performance?.now === 'function') return globalThis.performance.now();
  } catch {}
  try {
    if (typeof process !== 'undefined' && typeof process.hrtime?.bigint === 'function') {
      return Number(process.hrtime.bigint() / 1000000n);
    }
  } catch {}
  return Date.now();
}

/**
 * Creates a stateful function that generates decorrelated jittered backoff delays.
 * This strategy is effective at preventing thundering herds in distributed systems.
 * See: https://www.awsarchitectureblog.com/2015/03/backoff.html
 *
 * @param {number} base - The base sleep time in milliseconds (>= 0).
 * @param {number} max - The maximum sleep time in milliseconds (>= base).
 * @param {function(): number} [random=Math.random] - A function that returns a random number between 0 and 1.
 * @returns {function(): number} A function that, when called, returns the next sleep duration in milliseconds.
 */
export function createDecorrelatedJitter(base, max, random = Math.random) {
  let sleep = Math.max(0, base);
  return () => {
    const lo = Math.max(0, base);
    const hi = Math.max(lo, sleep * 3);
    const u = Math.max(0, Math.min(1, Number(random()) || 0));
    const next = lo + u * (hi - lo);
    sleep = Math.min(max, next);
    return sleep;
  };
}

/**
 * @typedef {Object} ErrorContext
 * @property {number} attempt - The zero-indexed attempt number for this run.
 * @property {number} retries - The configured total retries (not attempts).
 * @property {number} retriesLeft - How many retries remain after this attempt (>= 0).
 * @property {number} elapsedMs - Elapsed time since the run started, in milliseconds (monotonic).
 * @property {number} deadlineAtWall - Absolute timestamp (ms since epoch) when the overall deadline expires, or 0 if no deadline. For context/logging only; enforcement uses a monotonic clock.
 * @property {number} timeoutPerAttempt - Timeout for each attempt, in milliseconds (Infinity or 0 disable per-attempt timeout).
 * @property {number} overallDeadlineMs - Overall deadline in milliseconds (0 means "no overall deadline").
 * @property {number} [nextDelayMs] - The planned delay before the next attempt (only supplied to onError when a retry will happen).
 */

/**
 * @callback ShouldRetry
 * @param {any} err - The error thrown by the operation.
 * @param {number} attempt - The zero-indexed attempt number.
 * @param {ErrorContext} ctx - Additional context about the run.
 * @returns {boolean|Promise<boolean>} Whether to retry.
 */

/**
 * @callback OnError
 * @param {any} err - The error thrown by the operation.
 * @param {number} attempt - The zero-indexed attempt number.
 * @param {boolean} willRetry - Whether a retry will be attempted.
 * @param {ErrorContext} ctx - Additional context about the run (includes nextDelayMs if willRetry is true).
 * @returns {void|Promise<void>}
 */

/**
 * @typedef {Object} FinishInfo
 * @property {*} [result] - The successful result, if any.
 * @property {any} [error] - The final error, if any.
 * @property {number} attempts - Number of attempts actually performed.
 * @property {number} durationMs - Total runtime duration in milliseconds (monotonic).
 */

/**
 * @callback OnFinish
 * @param {FinishInfo} info - Summary of the run.
 * @returns {void|Promise<void>}
 */

/**
 * @callback GetDelay
 * @param {any} err - The error thrown by the operation (may include HTTP response/headers).
 * @param {number} attempt - The zero-indexed attempt number.
 * @param {ErrorContext} ctx - Additional context about the run.
 * @returns {number|null|undefined|Promise<number|null|undefined>} An override delay in ms. If null/undefined/NaN, the backoff strategy is used.
 */

// Common transient error codes (Node/OS-network typical)
const RETRYABLE_ERROR_CODES = new Set([
  'ECONNRESET',
  'ECONNREFUSED',
  'ECONNABORTED',
  'ETIMEDOUT',
  'ENETUNREACH',
  'EHOSTUNREACH',
  'EAI_AGAIN',
  'EPIPE',
  'ENOTFOUND',   // DNS resolution failures
  'EADDRINUSE'   // local port exhaustion
]);

/**
 * A utility class to execute an asynchronous operation with a robust policy for
 * retries, per-attempt timeouts, overall deadline, backoff, and jitter. It is
 * designed to be highly configurable and safe for use in any JavaScript environment.
 */
export class RobustOperation {
  /**
   * @param {object} [opts={}] - Configuration options for the operation.
   * @param {number} [opts.retries=3] - The maximum number of retries (total attempts = retries + 1). Clamped to >= 0.
   * @param {number} [opts.timeoutPerAttempt=15000] - Timeout for each attempt in milliseconds. Use Infinity or 0 to disable per-attempt timeout.
   * @param {number} [opts.overallDeadlineMs=0] - Overall deadline for all attempts in milliseconds (0 for no deadline). Clamped to >= 0.
   * @param {number} [opts.minDelay=0] - Minimum delay between retries in milliseconds (>= 0).
   * @param {number} [opts.maxDelay=30000] - Maximum delay between retries in milliseconds (>= minDelay).
   * @param {function(): number} [opts.random=Math.random] - A random number generator for jitter.
   * @param {number} [opts.backoffBase=1000] - The base backoff delay in milliseconds (>= 0).
   * @param {function(): number} [opts.backoffStrategy] - A function returning the next delay in milliseconds. Defaults to decorrelated jitter.
   * @param {ShouldRetry} [opts.shouldRetry] - A function to determine if a retry should occur.
   * @param {OnError} [opts.onError] - A callback for each error.
   * @param {GetDelay} [opts.getDelay] - Optional hook to override the computed delay (e.g., to honor Retry-After).
   * @param {OnFinish} [opts.onFinish] - Optional hook invoked once at the end of the run (success or failure) with summary metrics.
   */
  constructor(opts = {}) {
    // Core numeric configuration
    this.retries = Math.max(0, finiteOr(opts.retries, 3));
    // Allow Infinity or 0 to disable per-attempt timeout.
    this.timeoutPerAttempt = timeoutOr(opts.timeoutPerAttempt ?? opts.timeout, 15000);
    this.overallDeadlineMs = Math.max(0, finiteOr(opts.overallDeadlineMs, 0));
    this.minDelay = Math.max(0, finiteOr(opts.minDelay, 0));
    this.maxDelay = Math.max(this.minDelay, finiteOr(opts.maxDelay, 30000));

    // Random source
    this.random = typeof opts.random === 'function' ? opts.random : Math.random;

    // Backoff strategy
    const base = Math.max(0, finiteOr(opts.backoffBase ?? opts.backoff, 1000));
    this.backoffStrategy = typeof opts.backoffStrategy === 'function'
      ? opts.backoffStrategy
      : createDecorrelatedJitter(base, this.maxDelay, this.random);

    // Hooks
    this.shouldRetry = typeof opts.shouldRetry === 'function'
      ? opts.shouldRetry
      : this.defaultShouldRetry.bind(this);
    this.onError = typeof opts.onError === 'function' ? opts.onError : () => {};
    this.getDelay = typeof opts.getDelay === 'function' ? opts.getDelay : undefined;
    this.onFinish = typeof opts.onFinish === 'function' ? opts.onFinish : undefined;

    // Instance-level AbortController used by cancel()
    this._instanceController = new AbortController();
  }

  /**
   * The default classification logic to determine if an error is transient and should be retried.
   * - Aborts and integrity failures are not retried.
   * - Timeouts and common transient network/server errors are retried.
   * - HTTP 408, 429, and all 5xx (including 502/504) are retried by default.
   * - Unknown errors are retried at least once.
   *
   * @param {any} err - The error to classify.
   * @param {number} attempt - The zero-indexed attempt number.
   * @param {ErrorContext} _ctx - Context (unused by default).
   * @returns {boolean} `true` if the operation should be retried, `false` otherwise.
   */
  defaultShouldRetry(err, attempt, _ctx) {
    // Explicit non-retry: aborts and integrity failures
    if (err?.name === 'AbortError' || err?.code === 'ERR_ABORTED' || err?.isAbort) return false;
    if (err instanceof IntegrityError || err?.isIntegrityError) return false;

    // Timeouts (per-attempt) are typically transient
    if (err instanceof TimeoutError || err?.name === 'TimeoutError' || err?.isTimeout) return true;

    // HTTP status classification
    const status = err?.status ?? err?.response?.status;
    if (typeof status === 'number') {
      if (status === 408 || status === 429 || status >= 500) return true; // includes 500-599 such as 502/504
      if (status >= 400) return false;
    }

    // Common transient network/IO error codes
    const code = typeof err?.code === 'string' ? err.code.toUpperCase() : '';
    if (RETRYABLE_ERROR_CODES.has(code)) return true;

    // Fetch-like network errors
    if (err instanceof TypeError && /failed to fetch|network request failed/i.test(err.message)) return true;

    // Unknown errors: retry at least once
    return attempt < 1;
  }

  /**
   * Cancels all in-flight and future operations started via this instance.
   * This is permanent for the instance (the internal signal remains aborted).
   * Create a new instance if you need to run more operations after canceling.
   * @param {any} [reason] - Optional abort reason.
   */
  cancel(reason) {
    try { this._instanceController.abort(reason); } catch { this._instanceController.abort(); }
  }

  /**
   * Returns the instance-level AbortSignal that is aborted when `cancel()` is called.
   * Consumers can pass this to other APIs if desired.
   * @returns {AbortSignal}
   */
  get signal() {
    return this._instanceController.signal;
  }

  /**
   * Executes an operation with the configured retry and timeout logic.
   * The operation receives an AbortSignal and an object `{ attempt }` where `attempt` is zero-indexed.
   *
   * @template T
   * @param {function(AbortSignal, {attempt: number}): Promise<T>} operation - The async function to execute.
   * @param {object} [options={}] - Per-run options that can override the instance's configuration.
   * @param {number} [options.retries] - Override for retries for this run (clamped to >= 0).
   * @param {number} [options.timeoutPerAttempt] - Override for per-attempt timeout (Infinity or 0 disable per-attempt timeout).
   * @param {number} [options.overallDeadlineMs] - Override for overall deadline in ms (0 disables; clamped to >= 0).
   * @param {AbortSignal} [options.signal] - An external AbortSignal to cancel the entire operation.
   * @returns {Promise<T>} A promise that resolves with the result of the operation or rejects with the last error.
   */
  async run(operation, options = {}) {
    if (typeof operation !== 'function') throw new TypeError('operation must be a function');

    const retries = Math.max(0, finiteOr(options.retries, this.retries));
    const tOverride = options.timeoutPerAttempt ?? options.timeout;
    const timeoutPerAttempt = tOverride === undefined ? this.timeoutPerAttempt : timeoutOr(tOverride, this.timeoutPerAttempt);
    const dOverride = options.overallDeadlineMs;
    const overallDeadlineMs = dOverride === undefined ? this.overallDeadlineMs : Math.max(0, finiteOr(dOverride, this.overallDeadlineMs));
    const externalSignal = options.signal;

    // Use a monotonic start for accurate duration/deadline calculations
    const startMono = monotonicNowMs();
    // Provide a wall-clock deadline for observability only (internal logic is monotonic)
    const deadlineAtWall = overallDeadlineMs > 0 ? Date.now() + overallDeadlineMs : 0;

    /** @type {any} */
    let lastError;
    /** @type {any} */
    let finalResult;
    /** @type {any} */
    let finalError;
    let attemptsPerformed = 0;

    try {
      for (let attempt = 0; attempt <= retries; attempt++) {
        // Compute remaining budget using monotonic clock
        const elapsed = monotonicNowMs() - startMono;
        const budgetLeft = overallDeadlineMs ? Math.max(0, overallDeadlineMs - elapsed) : Infinity;
        if (overallDeadlineMs && budgetLeft <= 0) {
          throw lastError instanceof Error ? lastError : new TimeoutError('Overall deadline exceeded');
        }

        attemptsPerformed = attempt + 1;

        const attemptController = new AbortController();
        const { signal: composedSignal, cleanup } = RobustOperation.anySignal([
          attemptController.signal,
          externalSignal,
          this._instanceController.signal
        ]);

        let timerId = null;
        try {
          if (composedSignal.aborted) throw (composedSignal.reason ?? new Error('Aborted'));

          // Respect the remaining overall budget when calculating the per-attempt timeout.
          const perAttemptTimeout = Math.min(timeoutPerAttempt, budgetLeft);

          // Ensure sync exceptions are captured; also avoid unhandled rejections if timeout wins.
          const opPromise = Promise.resolve().then(() => operation(composedSignal, { attempt }));
          void opPromise.catch(() => {}); // prevent unhandled rejection if timer wins the race

          // Race with per-attempt timeout only if applicable.
          let result;
          if (perAttemptTimeout > 0 && perAttemptTimeout !== Infinity) {
            const timeoutPromise = new Promise((_, reject) => {
              timerId = setTimeout(() => {
                const te = new TimeoutError(`Attempt timed out after ${perAttemptTimeout}ms`);
                try { attemptController.abort(te); } catch {}
                reject(te);
              }, perAttemptTimeout);
            });
            result = await Promise.race([opPromise, timeoutPromise]);
          } else {
            result = await opPromise;
          }

          finalResult = result;
          return result;

        } catch (e) {
          lastError = e;
          finalError = e;
          // Ensure the in-flight operation sees the abort, regardless of error source.
          try { attemptController.abort(e); } catch {}

          // External or instance-level aborts always win.
          if (externalSignal?.aborted || this._instanceController.signal.aborted) throw e;

          // Decide whether to retry.
          const elapsedMs = monotonicNowMs() - startMono;
          const retriesLeft = Math.max(0, retries - attempt);
          /** @type {ErrorContext} */
          const baseCtx = {
            attempt,
            retries,
            retriesLeft,
            elapsedMs,
            deadlineAtWall,
            timeoutPerAttempt,
            overallDeadlineMs
          };

          let willRetry = false;
          if (attempt < retries) {
            try {
              willRetry = await this.shouldRetry(e, attempt, baseCtx);
            } catch {
              // If the hook throws, treat it as a decision to not retry.
              willRetry = false;
            }
          }

          // Compute next delay if retrying (honor getDelay, then Retry-After, then backoff).
          let nextDelayMs = 0;
          if (willRetry) {
            let delayCandidate;

            // 1) Hook override (allow 0 to mean "no wait"); ignore if it throws or returns invalid.
            if (typeof this.getDelay === 'function') {
              try {
                const override = await this.getDelay(e, attempt, baseCtx);
                const n = Number(override);
                if (override != null && Number.isFinite(n) && n >= 0) {
                  delayCandidate = n;
                }
              } catch {
                // ignore and fall back to other sources
              }
            }

            // 2) Retry-After header (for 429/503), if no override
            if (delayCandidate == null) {
              const inferred = inferRetryAfterMs(e, Date.now());
              if (typeof inferred === 'number') {
                delayCandidate = inferred;
              }
            }

            // 3) Backoff strategy, if still no delay selected
            if (delayCandidate == null) {
              delayCandidate = this.backoffStrategy();
            }

            // Enforce [minDelay, maxDelay], and respect remaining deadline for sleep.
            let dc = Number(delayCandidate);
            if (!Number.isFinite(dc)) dc = Infinity;
            dc = Math.max(0, dc);
            dc = Math.max(this.minDelay, Math.min(this.maxDelay, dc));

            const elapsed2 = monotonicNowMs() - startMono;
            const sleepBudget = overallDeadlineMs ? Math.max(0, overallDeadlineMs - elapsed2) : Infinity;
            nextDelayMs = Math.min(dc, sleepBudget);
          }

          // Invoke error callback (include nextDelayMs when retrying).
          try {
            const ctx = willRetry ? { ...baseCtx, nextDelayMs } : baseCtx;
            await this.onError(e, attempt, willRetry, ctx);
          } catch {
            // User-supplied onError should not break control flow.
          }

          if (!willRetry) throw e;

          // Sleep before next attempt (abortable by external and instance-level signals).
          const { signal: sleepSignal, cleanup: cleanupSleep } = RobustOperation.anySignal([
            externalSignal,
            this._instanceController.signal
          ]);
          try {
            await RobustOperation.abortableSleep(nextDelayMs, sleepSignal);
          } finally {
            try { cleanupSleep?.(); } catch {}
          }

        } finally {
          if (timerId != null) clearTimeout(timerId);
          try { cleanup?.(); } catch {}
        }
      }

      // If we get here, we've exhausted retries without returning.
      throw lastError;
    } catch (err) {
      finalError = err;
      throw err;
    } finally {
      // Single-shot finish hook with summary metrics (never throws).
      if (typeof this.onFinish === 'function') {
        const durationMs = monotonicNowMs() - startMono;
        try {
          await this.onFinish({
            result: finalResult,
            error: finalError,
            attempts: attemptsPerformed,
            durationMs
          });
        } catch {
          // Observability should never break control flow.
        }
      }
    }
  }

  /**
   * Composes multiple AbortSignals into a single signal that aborts when any of the input signals abort.
   * Caller must invoke the returned `cleanup()` to remove event listeners when the composed signal
   * is no longer needed (typically in a finally block).
   *
   * If no input signals are provided, a fresh, never-aborted signal is returned.
   *
   * @param {Array<AbortSignal|null|undefined>} [signals=[]] - An array of AbortSignal instances (falsy values ignored).
   * @returns {{signal: AbortSignal, cleanup: function(): void}} An object containing the combined signal and a cleanup function.
   */
  static anySignal(signals = []) {
    const active = signals.filter(s => s && typeof s.aborted === 'boolean');
    if (active.length === 0) {
      const c = new AbortController();
      return { signal: c.signal, cleanup: () => {} };
    }

    // If any source is already aborted, mirror that reason.
    const early = active.find(s => s.aborted);
    if (early) {
      const c = new AbortController();
      try { c.abort(early.reason); } catch { c.abort(); }
      return { signal: c.signal, cleanup: () => {} };
    }

    const c = new AbortController();
    /** @type {Array<() => void>} */
    const removers = [];
    let cleanup = () => { for (const rm of removers.splice(0)) { try { rm(); } catch {} } };

    for (const s of active) {
      const onAbort = () => {
        try { c.abort(s.reason); } catch { c.abort(); }
        cleanup();
      };
      try {
        s.addEventListener('abort', onAbort, { once: true });
        removers.push(() => { try { s.removeEventListener('abort', onAbort); } catch {} });
      } catch {}
    }

    return { signal: c.signal, cleanup };
  }

  /**
   * Returns a promise that resolves after a specified delay, but rejects if the provided signal is aborted.
   * @param {number} delayMs - The delay in milliseconds (negative values are treated as 0).
   * @param {AbortSignal} [signal] - An optional AbortSignal to cancel the sleep.
   * @returns {Promise<void>} A promise that resolves after the delay or rejects on abort.
   */
  static abortableSleep(delayMs, signal) {
    const d = Math.max(0, Number(delayMs) || 0);
    if (d === 0) {
      if (signal?.aborted) return Promise.reject(signal.reason ?? new Error('Aborted'));
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
      let tid = null;
      const onAbort = () => {
        if (tid != null) clearTimeout(tid);
        reject(signal?.reason ?? new Error('Aborted'));
      };
      if (signal) {
        if (signal.aborted) return onAbort();
        try { signal.addEventListener('abort', onAbort, { once: true }); } catch {}
      }
      tid = setTimeout(() => {
        try { signal?.removeEventListener?.('abort', onAbort); } catch {}
        resolve();
      }, d);
    });
  }
}

/**
 * Attempts to infer a retry delay from a Retry-After header or an attached `retryAfter` value.
 * Returns null if no valid delay could be inferred.
 * This is conservative and primarily used for 429/503 responses when available.
 * @param {any} err
 * @param {number} nowMs
 * @returns {number|null} delay in ms (>= 0) or null if unavailable/invalid
 */
function inferRetryAfterMs(err, nowMs) {
  try {
    const status = err?.status ?? err?.response?.status;
    if (status !== 429 && status !== 503) return null;

    // Direct numeric or string property
    const direct = err?.retryAfter;
    const directParsed = parseRetryAfterValue(direct, nowMs);
    if (typeof directParsed === 'number' && directParsed >= 0) return directParsed;

    // Try response headers
    const headers = err?.response?.headers ?? err?.headers;
    const value = getHeaderCaseInsensitive(headers, 'Retry-After');
    const headerParsed = parseRetryAfterValue(value, nowMs);
    if (typeof headerParsed === 'number' && headerParsed >= 0) return headerParsed;

    return null;
  } catch {
    return null;
  }
}

/**
 * Parses a Retry-After header value (seconds or HTTP-date) to milliseconds.
 * Returns null if invalid.
 * @param {any} value
 * @param {number} nowMs
 * @returns {number|null} milliseconds (>= 0) or null
 */
function parseRetryAfterValue(value, nowMs) {
  if (value == null) return null;
  const v = String(value).trim();
  if (!v) return null;

  // If numeric, interpret as seconds.
  if (/^\d+(\.\d+)?$/.test(v)) {
    const secs = Number(v);
    if (!Number.isFinite(secs) || secs < 0) return null;
    return Math.floor(secs * 1000);
  }

  // Otherwise, try date.
  const t = Date.parse(v);
  if (Number.isNaN(t)) return null;
  const delta = t - nowMs;
  return delta >= 0 ? delta : null;
}

/**
 * Retrieves a header value from various header shapes (Fetch Headers or plain object), case-insensitive.
 * @param {any} headers
 * @param {string} name
 * @returns {string|undefined}
 */
function getHeaderCaseInsensitive(headers, name) {
  if (!headers) return undefined;

  // Fetch Headers
  try {
    if (typeof headers.get === 'function') {
      const v = headers.get(name);
      return v == null ? undefined : String(v);
    }
  } catch {}

  // Plain object (case-insensitive)
  try {
    const entries = Object.entries(headers);
    for (const [k, v] of entries) {
      if (typeof k === 'string' && k.toLowerCase() === name.toLowerCase()) {
        return v == null ? undefined : String(v);
      }
    }
  } catch {}

  return undefined;
}

/**
 * A cryptographically secure random number generator to mimic Math.random().
 * Falls back to Math.random() if crypto.getRandomValues is not available.
 * @returns {number} A random floating-point number between 0 (inclusive) and 1 (exclusive).
 */
export const secureRandom = () => {
  try {
    const u32 = new Uint32Array(1);
    if (!globalThis.crypto?.getRandomValues) throw new Error('no crypto');
    globalThis.crypto.getRandomValues(u32);
    return u32[0] / 0x100000000;
  } catch {
    return Math.random();
  }
};

// --- EXAMPLE USAGE ---

/*
const roFetch = new RobustOperation({
  retries: 3,
  timeoutPerAttempt: 8000, // 8 seconds per try (use Infinity or 0 to disable per-attempt timeout)
  random: secureRandom,
  // Optional: override delay (e.g., to honor Retry-After with caps/custom logic)
  // getDelay: async (err, attempt, ctx) => { ... return msOrNull; },
  onError: (err, attempt, willRetry, ctx) => {
    const nth = attempt + 1;
    const msg = willRetry
      ? `Attempt ${nth} failed (${err?.message || err}). Retrying in ${ctx.nextDelayMs}ms...`
      : `Attempt ${nth} failed (${err?.message || err}). No more retries.`;
    console.warn(msg);
  },
  onFinish: ({ result, error, attempts, durationMs }) => {
    console.log('Run finished', { ok: !error, attempts, durationMs });
  }
});

// Later, you can cancel all operations started via this instance:
// roFetch.cancel(new Error('Shutting down'));

// A mock API function that sometimes fails
let requestCounter = 0;
async function fetchSomeData(url, signal) {
  console.log(`Attempting to fetch ${url}...`);
  // Simulate a network delay
  await new Promise(res => setTimeout(res, 500));

  requestCounter++;
  if (requestCounter <= 2) {
    // Simulate a transient server error
    const err = new Error("Server is temporarily unavailable");
    err.status = 503;
    throw err;
  }

  // Simulate success
  return { data: `Success from ${url} on attempt ${requestCounter}` };
}

// Run the operation
async function main() {
  try {
    const result = await roFetch.run(
      (signal, { attempt }) => fetchSomeData('https://api.example.com/data', signal)
    );
    console.log('Operation succeeded:', result);
  } catch (e) {
    console.error('Operation failed after all retries:', e.message);
  }
}

main();
*/