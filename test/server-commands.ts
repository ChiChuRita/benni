import { node } from "../src/node/index.js";

/**
 * Which of `names` the server at `url` implements, asked of the server itself
 * with COMMAND INFO (an unknown command answers nil). CI runs the suite against
 * Redis 7.2 and Valkey 8 as well as Redis 8, so a test for a newer command is
 * gated on the command, not on a version number: Valkey reports
 * redis_version 7.2.4 whatever it implements, and a capability check reads
 * right on every server.
 */
export async function serverCommands(
  url: string | undefined,
  names: readonly string[]
): Promise<ReadonlySet<string>> {
  if (url === undefined) return new Set();
  const client = await node({ url });
  try {
    const info = (await client.send([
      "COMMAND",
      "INFO",
      ...names
    ])) as unknown[];
    return new Set(names.filter((_, index) => info[index] != null));
  } finally {
    await client.close();
  }
}
