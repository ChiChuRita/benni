import { createServer } from "node:net";

/**
 * A TCP port nothing is listening on, for tests that need a connect to fail.
 * A hard-coded "unused" port is only unused until something on the machine
 * binds it, and then the test connects to a stranger instead of failing. Bind
 * port 0, let the OS pick a free one, and release it.
 *
 * The port is free when this resolves, not guaranteed free forever: another
 * process could take it before the test dials. That window is microseconds on
 * a loopback port the OS just handed out, which is as good as it gets without
 * holding the socket open (and a held listener would accept the connection).
 */
export function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.unref();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        server.close();
        reject(new Error("could not read the bound port"));
        return;
      }
      server.close(() => resolve(address.port));
    });
  });
}
