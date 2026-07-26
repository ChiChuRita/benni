import { expectNumber } from "./helpers.js";
import type { Codec, RedisClient } from "./types.js";

export type PubSubChannel<
  TInput,
  TOutput = TInput,
  TName extends string = string
> = {
  readonly kind: "channel";
  readonly name: TName;
  encode(message: TInput): string;
  decode(message: string): TOutput;
};

export type PubSubHandler<TMessage> = (
  message: TMessage
) => void | Promise<void>;

export type PubSubPattern<TOutput, TPattern extends string = string> = {
  readonly kind: "pattern";
  readonly pattern: TPattern;
  decode(message: string): TOutput;
};

export type PubSubPatternHandler<TMessage> = (
  message: TMessage,
  channel: string
) => void | Promise<void>;

export type PubSubSubscription = {
  unsubscribe(): Promise<void>;
};

export type InferPubSubInput<TChannel> =
  TChannel extends PubSubChannel<infer TInput> ? TInput : never;

export function definePubSubChannel<
  TName extends string,
  TInput,
  TOutput = TInput
>(
  name: TName,
  codec: Codec<TInput, TOutput>
): PubSubChannel<TInput, TOutput, TName> {
  return {
    kind: "channel",
    name,
    encode(message) {
      return codec.encode(message);
    },
    decode(message) {
      return codec.decode(message);
    }
  };
}

export function definePubSubPattern<
  TPattern extends string,
  TInput,
  TOutput = TInput
>(
  pattern: TPattern,
  codec: Codec<TInput, TOutput>
): PubSubPattern<TOutput, TPattern> {
  return {
    kind: "pattern",
    pattern,
    decode(message) {
      return codec.decode(message);
    }
  };
}

export function createPubSubPublisher(client: RedisClient) {
  return {
    async publish<TChannel extends PubSubChannel<any>>(
      channel: TChannel,
      message: InferPubSubInput<TChannel>
    ): Promise<number> {
      return expectNumber(
        await client.send(["PUBLISH", channel.name, channel.encode(message)]),
        "PUBLISH"
      );
    }
  };
}
