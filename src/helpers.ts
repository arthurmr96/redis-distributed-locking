import { randomUUID } from "node:crypto";
import { RedisClientType } from "redis";

export type Lock = {
  resourceName: string;
  key: string;
  value: string;
}

export type JobExecution = {
  resourceName: string;
  ttl: number;
  job: (params: Record<string, unknown>, signal: AbortSignal) => Promise<void>;
  params: Record<string, unknown>;
  redisClient: RedisClientType;
}

export async function acquire(resourceName: string, ttl: number, redisClient: RedisClientType): Promise<Lock | null> {
  const lockKey = `lock:${resourceName}`;
  const lockValue = randomUUID();
  const set = await redisClient.set(lockKey, lockValue, {
    NX: true,
    PX: ttl,
  });

  if (set === null) {
    return null; // Lock not acquired
  }

  return { resourceName, key: lockKey, value: lockValue };
} 

export async function release(lock: Lock, redisClient: RedisClientType): Promise<boolean> {
  const luaEval = `
    if redis.call("get", KEYS[1]) == ARGV[1] then
      return redis.call("del", KEYS[1])
    else
      return 0
    end
  `;

  const result = await redisClient.eval(luaEval, {
    keys: [lock.key],
    arguments: [lock.value],
  });

  if (result === 0) {
    return false
  }

  return true;
}

export async function markComplete(lock: Lock, redisClient: RedisClientType): Promise<boolean> {
  const luaEval = `
    if redis.call("get", KEYS[1]) == ARGV[1] then
      redis.call("set", KEYS[2], "true", "PX", 30000) -- Set a key to indicate completion with a TTL of 30 seconds
      return 1
    else
      return 0
    end
  `;

  const result = await redisClient.eval(luaEval, {
    keys: [lock.key, `done:${lock.resourceName}`],
    arguments: [lock.value],
  });

  return result === 1;
}

export async function renew(lock: Lock, ttl: number, redisClient: RedisClientType): Promise<boolean> {
  const luaEval = `
    if redis.call("get", KEYS[1]) == ARGV[1] then
      return redis.call("pexpire", KEYS[1], ARGV[2])
    else
      return 0
    end
  `;

  const result = await redisClient.eval(luaEval, {
    keys: [lock.key],
    arguments: [lock.value, String(ttl)],
  });

  if (result === 0) {
    return false; // Lock not renewed
  }

  return true; // Lock renewed
}

export async function executeJob(jobExecution: JobExecution): Promise<void> {
  const { resourceName, ttl, job, params, redisClient } = jobExecution;
  const isCompleted = await redisClient.get(`done:${resourceName}`);

  if (isCompleted) {
    console.log(`Job for resource ${resourceName} has already been completed.`);
    return;
  }

  const lock = await acquire(resourceName, ttl, redisClient);

  if (!lock) {
    console.log(`Resource ${resourceName} is already locked.`);
    return;
  }

  const abortController = new AbortController();
  let renewInterval: NodeJS.Timeout | undefined = undefined;

  console.log(`Lock acquired for resource: ${resourceName}. Executing job...`);

  try {
    const jobPromise = job(params, abortController.signal);
    renewInterval = setInterval(async () => {
      const renewed = await renew(lock, ttl, redisClient);
      if (!renewed) {
        console.log(`Lock lost for resource: ${resourceName}. Aborting job...`);
        clearInterval(renewInterval);
        abortController.abort();
      } else {
        console.log(`Lock renewed for resource: ${resourceName}.`);
      }
    }, ttl / 2);

    await jobPromise;
    if (abortController.signal.aborted) {
      console.log(`Job for resource ${resourceName} was aborted due to lock loss.`);
      return
    }

    const completed = await markComplete(lock, redisClient);
      if (completed) {
        console.log(`Job completed for resource: ${resourceName}.`);
      } else {
        console.log(`Lock was lost before completion for resource: ${resourceName}.`);
      }
  } catch (error) {
    console.error(`Job failed for resource: ${resourceName}.`, error);
  } finally {
    if (renewInterval) {
      clearInterval(renewInterval);
    }
    
    const released = await release(lock, redisClient);

    if (released) {
      console.log(`Lock released for resource: ${resourceName}.`);
    } else {
      console.log(`Lock was already lost for resource: ${resourceName}.`);
    }
  }
}  

export async function runForLeader(resourceName: string, ttl: number, redisClient: RedisClientType): Promise<void> {
  // let's keep a renew interval to renew the execution. if the process is not the leader, it should still be running as a backup to take up as leader if the leader fails. This is a common pattern in distributed systems to ensure high availability.
  let lock: Lock | null = null;
  let renewInterval: NodeJS.Timeout | undefined;

  function stopRenewInterval(): void {
    if (renewInterval) {
      clearInterval(renewInterval);
      renewInterval = undefined;
    }
  }

  function startRenewInterval(): NodeJS.Timeout {
    return setInterval(async () => {
      if (!lock) {
        return;
      }

      try {
        const renewal = await renew(lock, ttl, redisClient);

        if (renewal) {
          console.log(
            `Renewed leadership for resource: ${resourceName}.`,
          );
        } else {
          console.log(
            `Failed to renew leadership for resource: ${resourceName}. Lock may have been lost.`,
          );

          stopRenewInterval();
          lock = null;
        }
      } catch (error) {
        console.error(
          `Failed to renew leadership for resource: ${resourceName}.`,
          error,
        );

        stopRenewInterval();
        lock = null;
      }
    }, ttl / 3);
  }

  while (true) {
    try {
      if (!lock) {
        lock = await acquire(`leader:${resourceName}`, ttl, redisClient);
        if (!lock) {
          console.log(`Resource ${resourceName} is already locked. Waiting for leadership...`);
        } else {
          renewInterval = startRenewInterval();
          console.log(`Acquired leadership for resource: ${resourceName}.`);
        }

      } else {
        console.log(`Already holding leadership for resource: ${resourceName}. Continuing execution...`);
        await executeJob({
          resourceName,
          ttl,
          job: async (params, signal) => {
            // Simulate a long-running job
            for (let i = 0; i < 15; i++) {
              if (signal.aborted) {
                console.log(`Job for resource ${resourceName} was aborted.`);
                return;
              }
              console.log(`Executing job for resource ${resourceName}: step ${i + 1}`);
              await new Promise(resolve => setTimeout(resolve, 1000)); // Simulate work
            }
          },
          params: {},
          redisClient,
        });
      }
    } catch (error) {
      console.error(`Error during leadership execution for resource ${resourceName}:`, error);
      if (lock) {
        const released = await release(lock, redisClient);
        if (released) {
          console.log(`Lock released for resource: ${resourceName} due to error.`);
        } else {
          console.log(`Lock was already lost for resource: ${resourceName} due to error.`);
        }
        stopRenewInterval();
        lock = null;
      }
    }

    await new Promise(resolve => setTimeout(resolve, ttl / 2)); // Wait before next attempt
  }
  
}