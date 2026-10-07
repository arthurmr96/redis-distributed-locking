import { createClient } from "redis";
import { executeJob } from "./helpers";

async function main(): Promise<void> {
  const client = createClient({
    url: process.env.REDIS_URL ?? "redis://localhost:6379",
  });

  client.on("error", (error: Error) => {
    console.error("Redis client error:", error.message);
  });

  try {
    await client.connect();
    const resourceName = "my-resource";
    const job = async (params: Record<string, unknown>, signal: AbortSignal): Promise<void> => {
      console.log(`Job started with params: ${JSON.stringify(params)}`);

      for (let step = 0; step < 15; step++) {
        if (signal.aborted) {
          console.log("Job stopped because the lock was lost.");
          return;
        }

        await new Promise((resolve) => setTimeout(resolve, 1000));

        if (signal.aborted) {
          console.log("Job stopped because the lock was lost.");
          return;
        }

        console.log(`Job progress: ${step + 1}/15`);
      }

      console.log("Job completed.");
    };

    const jobExecution = {
      resourceName,
      ttl: 10000, // Lock TTL in milliseconds
      job,
      params: { key: "value" },
      redisClient: client,
    };

    await executeJob(jobExecution);
    
  }
  catch (error: unknown) {
    console.error(error)
  }
  finally {
    if (client.isOpen) {
      await client.quit();
    }
  }
}

void main().catch((error: unknown) => {
  console.error("Unable to connect to Redis:", error);
  process.exitCode = 1;
});