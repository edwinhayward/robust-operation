import { test } from 'node:test';
import assert from 'node:assert';
import { RobustOperation } from './robust_operation.js';

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