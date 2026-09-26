import { connect, createServer, type Server, type Socket } from "node:net";
import { freePort } from "./free-port.js";

/**
 * A loopback TCP forwarder in front of a real Redis, for the failures a test
 * cannot otherwise stage: a server that is not up yet when the client first
 * dials, and a connection that drops under a client that was connected.
 *
 * `port` is reserved up front and nothing listens on it until `start()`, so a
 * client pointed at it fails to connect exactly like one booting while Redis
 * restarts.
 */
export type TcpProxy = {
  readonly port: number;
  /** Start accepting and forwarding to the upstream Redis. */
  start(): Promise<void>;
  /** Destroy every forwarded connection; the listener stays up. */
  dropConnections(): void;
  /** Stop listening and destroy every forwarded connection. */
  stop(): Promise<void>;
};

export async function tcpProxy(upstreamUrl: string): Promise<TcpProxy> {
  const upstream = new URL(upstreamUrl);
  const port = await freePort();
  const sockets = new Set<Socket>();
  let server: Server | undefined;

  return {
    port,
    start() {
      server = createServer((client) => {
        const remote = connect(
          Number(upstream.port || 6379),
          upstream.hostname
        );
        for (const socket of [client, remote]) {
          sockets.add(socket);
          socket.on("close", () => sockets.delete(socket));
          socket.on("error", () => {
            client.destroy();
            remote.destroy();
          });
        }
        client.pipe(remote).pipe(client);
      });
      return new Promise((resolve, reject) => {
        server?.once("error", reject);
        server?.listen(port, "127.0.0.1", () => resolve());
      });
    },
    dropConnections() {
      for (const socket of sockets) socket.destroy();
    },
    stop() {
      for (const socket of sockets) socket.destroy();
      return new Promise((resolve) => {
        if (server === undefined) resolve();
        else server.close(() => resolve());
      });
    }
  };
}
