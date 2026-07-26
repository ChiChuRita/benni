// Minimal ambient surface of Bun's built-in Redis client, as used by the Bun
// runtime adapter in ./index.ts. Declared locally instead of depending on
// @types/bun: including bun-types in this compilation overrides shared globals
// and breaks type-level assertions elsewhere in the repo under
// typescript@6 + @types/node@26.
declare namespace Bun {
  interface RedisOptions {
    connectionTimeout?: number;
    idleTimeout?: number;
    autoReconnect?: boolean;
    maxRetries?: number;
    enableOfflineQueue?: boolean;
    tls?: boolean;
    enableAutoPipelining?: boolean;
  }

  class RedisClient {
    constructor(url?: string, options?: RedisOptions);
    readonly connected: boolean;
    connect(): Promise<void>;
    close(): void;
    send(command: string, args: Array<string | Uint8Array>): Promise<unknown>;
    subscribe(
      channel: string,
      listener: (message: string, channel: string) => void
    ): Promise<number>;
    unsubscribe(
      channel: string,
      listener: (message: string, channel: string) => void
    ): Promise<void>;
  }
}
