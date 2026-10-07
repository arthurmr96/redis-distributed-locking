import { createClient } from "redis";
import { runForLeader } from "./helpers";

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
    const ttl = 10000; // 10 seconds

    await runForLeader(resourceName, ttl, client);
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