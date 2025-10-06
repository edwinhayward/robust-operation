import { test } from 'node:test';
import assert from 'node:assert';
import { RobustOperation, createDecorrelatedJitter } from './robust_operation.js';

test('createDecorrelatedJitter should not get stuck at zero', () => {
  const maxDelay = 100;
  const jitter = createDecorrelatedJitter(0, maxDelay, () => 0.5); // Use a predictable random generator

  const results = Array.from({ length: 5 }, () => jitter());

  // With a base of 0, the first result will be 0.
  // The bug is that all subsequent results are also 0.
  // The fix should ensure that subsequent results can be > 0.
  const sum = results.reduce((a, b) => a + b, 0);

  assert.ok(sum > 0, 'The jitter function should produce non-zero delays eventually');
  results.forEach(r => {
    assert.ok(r >= 0 && r <= maxDelay, 'Each result should be within the [0, max] range');
  });
});

test('should use a zero delay for a Retry-After date in the past', async () => {
  let capturedDelay = -1;
  const pastDate = new Date(Date.now() - 10000).toUTCString();

  const ro = new RobustOperation({
    retries: 1,
    minDelay: 0, // Ensure no artificial minimum delay interferes
    backoffBase: 5000, // Make the default backoff high to contrast with 0
    onError: (err, attempt, willRetry, ctx) => {
      if (willRetry) {
        capturedDelay = ctx.nextDelayMs;
      }
    },
  });

  const errorToThrow = new Error('Throttled');
  errorToThrow.status = 429;
  errorToThrow.headers = { 'Retry-After': pastDate };

  try {
    await ro.run(async () => {
      throw errorToThrow;
    });
  } catch (e) {
    // Expected to fail after retries
  }

  assert.strictEqual(capturedDelay, 0, 'The retry delay for a past date should be 0ms');
});