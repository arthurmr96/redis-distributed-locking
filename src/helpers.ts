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

export async function renew(lock: Lock, ttl: number, redisClient: RedisClientType): Promise<void> {
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
    throw new Error(`Failed to renew lock for resource ${lock.resourceName}.`);
  }
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
      try {
        await renew(lock, ttl, redisClient);
        console.log(`Lock renewed for resource: ${resourceName}.`);
      } catch (error) {
        console.error(`Failed to renew lock for resource: ${resourceName}.`, error);
        clearInterval(renewInterval);
        abortController.abort();
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
