# Robust Operation

A tiny, zero-dependency utility to run asynchronous operations with:
- Per-attempt timeouts
- An overall deadline
- Retry policy with configurable classification
- Jittered backoff (decorrelated jitter)
- AbortSignal composition (abort when any of multiple signals abort)
- Observability hooks

Lean, predictable, and safe for both browser and Node environments. Internally uses a monotonic clock to avoid deadline drift from wall-clock changes.

Version: 1.0.0

---

## Table of Contents

- Why
- Features
- Install
- Quick Start
- API
  - Class RobustOperation
  - Options
  - Hooks
  - Methods
  - Static Utilities
  - Helper Functions
  - Error Types
- Behavioral Details
- Examples
  - Resilient fetch
  - Respect Retry-After
  - External abort
  - Instance cancel
  - Custom retry classification
- TypeScript
- Notes on clocks, deadlines, and memory safety
- FAQ
- License

---

## Why

Most retry helpers stop at “retry n times with exponential backoff.” This utility goes further:
- Per-attempt timeouts and an overall deadline
- Jittered backoff tuned to avoid thundering herds
- AbortSignal composition so attempts and sleeps can be canceled immediately
- Sensible default classification for transient errors, with hooks for full control
- Metrics hook (`onFinish`) for observability
- Monotonic time internally, so wall-clock changes don’t surprise you

---

## Features

- Per-attempt timeout (or disable it with 0 or Infinity)
- Overall deadline across all attempts
- Decorrelated jitter backoff with min/max caps
- Retry classification with good defaults:
  - HTTP 408/429/5xx (including 502/504)
  - Common Node network error codes (ECONNRESET, ETIMEDOUT, ENOTFOUND, EADDRINUSE, …)
  - Fetch-like network failures
- Honors Retry-After for 429/503 when present
- AbortSignal composition, including instance-level cancel (fire-and-forget then cancel later)
- Small surface area; zero dependencies

---

## Install

Use directly as an ES module in your project.

- Local file import:
  ```js
  import { RobustOperation, secureRandom } from './robust-operation.js';
  ```

- If you publish to npm later, usage will look like:
  ```js
  import { RobustOperation, secureRandom } from 'robust-operation';
  ```

This library is ESM-first.

---

## Quick Start

```js
import { RobustOperation, secureRandom } from './robust-operation.js';

const op = new RobustOperation({
  retries: 3,
  timeoutPerAttempt: 8000, // 8s per attempt; 0 or Infinity disables per-attempt timeout
  random: secureRandom,
  onError: (err, attempt, willRetry, ctx) => {
    const nth = attempt + 1;
    console.warn(
      willRetry
        ? `Attempt ${nth} failed: ${err?.message || err}. Retrying in ${ctx.nextDelayMs}ms...`
        : `Attempt ${nth} failed: ${err?.message || err}. No more retries.`
    );
  },
  onFinish: ({ error, attempts, durationMs }) => {
    console.log('Finished', { ok: !error, attempts, durationMs });
  }
});

const result = await op.run(async (signal, { attempt }) => {
  // your async operation here; check `signal.aborted` or pass it to fetch()
  await fetch('https://api.example.com/data', { signal });
  return 'ok';
});
```

---

## API

### Class `RobustOperation`

Constructs a policy-bound runner for asynchronous operations.

```js
new RobustOperation(options?)
```

#### Options

All numeric values are clamped to sensible ranges. Defaults are shown.

- `retries: number = 3`
  - Number of retries. Total attempts = `retries + 1`. Must be >= 0.

- `timeoutPerAttempt: number = 15000`
  - Milliseconds per attempt. Use `0` or `Infinity` to disable per-attempt timeout.

- `overallDeadlineMs: number = 0`
  - Milliseconds for the entire run. `0` disables the overall deadline.

- `minDelay: number = 0`
  - Minimum delay between retries, in ms.

- `maxDelay: number = 30000`
  - Maximum delay between retries, in ms.

- `random: () => number = Math.random`
  - Random source for jitter. Consider `secureRandom()`.

- `backoffBase: number = 1000`
  - Base delay for the built-in decorrelated jitter strategy.

- `backoffStrategy?: () => number`
  - Provide your own delay generator. Defaults to decorrelated jitter.

- `shouldRetry?: (err, attempt, ctx) => boolean | Promise<boolean>`
  - Decide whether to retry on a given error/attempt. See default behavior below.

- `getDelay?: (err, attempt, ctx) => number | null | undefined | Promise<number | null | undefined>`
  - Optionally override the next delay (e.g., to honor or clamp `Retry-After`). Return `0` to retry immediately.

- `onError?: (err, attempt, willRetry, ctx) => void | Promise<void>`
  - Called on each failure; useful for logging/metrics.

- `onFinish?: ({ result, error, attempts, durationMs }) => void | Promise<void>`
  - Called once when the run finishes (success or failure). Never throws.

##### ErrorContext (`ctx`)

- `attempt: number` — zero-indexed attempt number
- `retries: number`
- `retriesLeft: number`
- `elapsedMs: number` — monotonic duration since `run` started
- `deadlineAtWall: number` — absolute ms since epoch when the overall deadline would expire, for logging; enforcement uses a monotonic clock
- `timeoutPerAttempt: number`
- `overallDeadlineMs: number`
- `nextDelayMs?: number` — planned delay before next attempt (present only when `willRetry` is true)

#### Hooks: Default Behavior

`shouldRetry` (default):
- Not retried: aborts, `IntegrityError`
- Retried: `TimeoutError`, HTTP 408/429/5xx (including 502/504)
- Retried: common transient Node error codes:
  - `ECONNRESET`, `ECONNREFUSED`, `ECONNABORTED`, `ETIMEDOUT`, `ENETUNREACH`, `EHOSTUNREACH`, `EAI_AGAIN`, `EPIPE`, `ENOTFOUND`, `EADDRINUSE`
- Retried: fetch-like network errors (`TypeError` with “Failed to fetch” / “Network request failed”)
- Unknown errors: retried once

`getDelay` (default none):
- If not provided or returns `null`/`undefined`/NaN, delay falls back to:
  1) `Retry-After` for 429/503 (seconds or HTTP-date)
  2) Backoff strategy (decorrelated jitter)
- `minDelay`/`maxDelay` are enforced.

#### Methods

- `run(operation, options?) => Promise<T>`

  Runs your async `operation(signal, { attempt })`. The `signal` will be aborted on:
  - External abort
  - Instance-level `cancel()`
  - Per-attempt timeout
  - Overall deadline expiry

  Per-run `options` can override:
  - `retries`
  - `timeoutPerAttempt`
  - `overallDeadlineMs`
  - `signal` (external AbortSignal)

- `cancel(reason?) => void`

  Aborts all in-flight operations started by this instance and prevents future runs from proceeding. Create a new instance to run more operations later.

- `signal: AbortSignal` (getter)

  An instance-level signal that becomes aborted when `cancel()` is called. You can pass this to your own code if needed.

#### Static Utilities

- `RobustOperation.anySignal(signals?) => { signal: AbortSignal, cleanup: () => void }`

  Compose multiple signals into one that aborts when any source aborts. Always call `cleanup()` when done to remove listeners.

- `RobustOperation.abortableSleep(delayMs, signal?) => Promise<void>`

  Sleep for `delayMs`, rejecting early if `signal` aborts.

#### Helper Functions

- `createDecorrelatedJitter(baseMs, maxMs, random?) => () => number`

  Returns a function that yields the next delay using Amazon’s decorrelated jitter strategy.

- `secureRandom() => number`

  Returns a cryptographically strong random float in [0, 1), falling back to `Math.random()` if `crypto.getRandomValues` is not available.

#### Error Types

- `TimeoutError`
- `IntegrityError`

---

## Behavioral Details

- Per-attempt timeout
  - Set `timeoutPerAttempt` to `0` or `Infinity` to disable.
  - If the per-attempt timer fires, `operation`’s signal is aborted and a `TimeoutError` is raised.

- Overall deadline
  - Applies across all attempts including sleep between retries.
  - Enforced with a monotonic clock to avoid wall-clock jumps.
  - Sleep is clamped so it won’t overshoot the remaining budget.

- Backoff and jitter
  - Default is decorrelated jitter with `backoffBase` and `maxDelay`.
  - `minDelay` and `maxDelay` are always enforced.

- Retry classification
  - Sensible defaults; fully customizable via `shouldRetry`.

- Retry-After
  - For HTTP 429/503, if `Retry-After` is present, it is used unless `getDelay` provides an override.

- Abort behavior
  - `operation` receives a composed signal that aborts on: external signal, instance `cancel()`, or per-attempt timeout.
  - Sleep between attempts can also be externally or instance-aborted.

---

## Examples

### 1) Resilient fetch

```js
import { RobustOperation, secureRandom } from './robust-operation.js';

const roFetch = new RobustOperation({
  retries: 3,
  timeoutPerAttempt: 8000,
  random: secureRandom,
  onError: (err, attempt, willRetry, ctx) => {
    console.warn(
      `Attempt ${attempt + 1} failed: ${err?.message || err}.` +
      (willRetry ? ` Retrying in ${ctx.nextDelayMs}ms...` : ' Giving up.')
    );
  }
});

const result = await roFetch.run(async (signal) => {
  const res = await fetch('https://api.example.com/data', { signal });
  if (!res.ok) {
    const err = new Error(`HTTP ${res.status}`);
    err.status = res.status;
    err.response = res;
    throw err;
  }
  return await res.json();
});

console.log('Got data:', result);
```

### 2) Respect Retry-After

```js
const op = new RobustOperation({
  retries: 5,
  getDelay: (err) => {
    // Clamp any Retry-After (if present) to at most 10s. If not present, return null to use the built-in backoff.
    // Note: built-in logic automatically uses Retry-After for 429/503. This example shows how to add a cap.
    const h = err?.response?.headers?.get?.('Retry-After') ?? err?.retryAfter;
    if (h && /^\d+(\.\d+)?$/.test(String(h))) {
      return Math.min(Number(h) * 1000, 10_000);
    }
    return null; // fall back to built-in inference + jitter strategy
  }
});
```

### 3) External abort

```js
const controller = new AbortController();

const op = new RobustOperation({ retries: 10, timeoutPerAttempt: 5000 });

const promise = op.run(async (signal) => {
  // This fetch will be aborted if controller.abort() is called
  const res = await fetch('https://example.com/slow', { signal });
  return res.text();
}, { signal: controller.signal });

// Cancel from outside later:
controller.abort(new Error('No longer needed'));

await promise; // will reject with an abort reason
```

### 4) Instance cancel

```js
const op = new RobustOperation({ retries: 100 });

// Kick off work you might cancel later:
const p1 = op.run(work);
const p2 = op.run(work);

// Cancel all runs started by this instance:
op.cancel(new Error('Shutting down'));

// Both p1 and p2 will reject promptly
```

### 5) Custom retry classification

```js
const op = new RobustOperation({
  retries: 4,
  shouldRetry: (err, attempt, ctx) => {
    // Never retry on 4xx except 408/429; retry on 5xx
    const status = err?.status ?? err?.response?.status;
    if (status && status >= 400 && status < 500 && status !== 408 && status !== 429) {
      return false;
    }
    return attempt < ctx.retries; // equivalent to attempt < 4 here
  }
});
```

---

## TypeScript

- The codebase is well-annotated with JSDoc, and a lightweight `index.d.ts` re-exports the module’s symbols to improve TS friendliness:

  ```ts
  export * from './robust-operation.js';
  ```

- Usage in TS:

  ```ts
  import { RobustOperation, TimeoutError } from './robust-operation.js';

  const op = new RobustOperation({ retries: 2 });

  const value = await op.run<string>(async (signal) => {
    // ...
    return 'ok';
  });
  ```

Note: For the best developer experience in a published package, consider shipping full `.d.ts` files generated from these JSDoc types.

---

## Notes on clocks, deadlines, and memory safety

- Monotonic vs wall-clock:
  - Deadline enforcement uses a monotonic clock (`performance.now()` or `process.hrtime.bigint()`), so system time changes won’t extend or cut your deadlines unexpectedly.
  - `ErrorContext.deadlineAtWall` is provided for logging/observability only.

- Memory safety:
  - `anySignal()` returns a `cleanup()` function; always call it (the library does so internally).
  - Long-lived processes: prefer reusing a `RobustOperation` instance rather than creating thousands of them dynamically.

---

## FAQ

- Why “decorrelated jitter”?
  - It spreads retries across time more effectively than naive exponential backoff, reducing load spikes under failure.

- Does abort really cancel my operation?
  - Your operation receives an `AbortSignal`. Pass it to APIs that support it (e.g., `fetch`) or check `signal.aborted` yourself and stop promptly.

- What if my operation ignores the signal?
  - The library aborts the signal and raises the timeout, but if your operation doesn’t respect abort, any internal work might still continue. Always design your operations to be abort-aware.

- Can I configure total attempts instead of `retries + 1`?
  - Today you configure `retries`. A `maxAttempts` alias may be added in the future for symmetry with some APIs.

---

## A Note on Stability

Please be aware that this is an early-stage project. While I've done my best to make the code functional and correct, it has not yet been validated by an automated test suite.

I encourage you to try it out, but please test it thoroughly within your own application before relying on it for anything critical.

The project's top priority is to add tests, and any contributions in this area are very welcome.

---

## License

This project is licensed under the MIT License.

**Note on Authorship:** Portions of this Software may have been generated with the assistance of AI tools. Final authorship, responsibility, and copyright for the Software rest with the copyright holder.

---

**MIT License**

Copyright (c) 2025 Edwin Hayward, Genki Productions Ltd

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.