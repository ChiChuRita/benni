// Standard (RFC 4648, padded) base64, shared by the `bytes()` codec, the
// Upstash adapter's response decoding, and benni/hono's cache entries.
//
// Hand-rolled base64, NOT Uint8Array.toBase64/fromBase64. Those are
// runtime-missing on the Node 24 baseline (present in the type lib but throw at
// runtime — CI caught it); keep this until the minimum Node has them unflagged.
const base64Alphabet =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

const base64Values = new Map<string, number>(
  [...base64Alphabet].map((char, index) => [char, index])
);

export function encodeBase64(input: Uint8Array): string {
  let encoded = "";
  for (let index = 0; index < input.length; index += 3) {
    const first = input[index];
    const second = index + 1 < input.length ? input[index + 1] : undefined;
    const third = index + 2 < input.length ? input[index + 2] : undefined;
    encoded += base64Alphabet[first >> 2];
    encoded += base64Alphabet[((first & 0b11) << 4) | ((second ?? 0) >> 4)];
    encoded +=
      second === undefined
        ? "="
        : base64Alphabet[((second & 0b1111) << 2) | ((third ?? 0) >> 6)];
    encoded += third === undefined ? "=" : base64Alphabet[third & 0b111111];
  }
  return encoded;
}

/**
 * The bytes `stored` encodes, or `undefined` when it is not well-formed
 * padded base64. Callers decide what malformed means for them: a codec
 * reports a shape error, a transport falls back to the raw text.
 */
export function decodeBase64(stored: string): Uint8Array | undefined {
  if (stored === "") return new Uint8Array();
  if (stored.length % 4 !== 0) {
    return undefined;
  }
  const padding = stored.endsWith("==") ? 2 : stored.endsWith("=") ? 1 : 0;
  const body = stored.slice(0, stored.length - padding);
  if (body.includes("=")) {
    return undefined;
  }
  const decoded = new Uint8Array((stored.length / 4) * 3 - padding);
  let decodedIndex = 0;
  let buffer = 0;
  let bufferedBits = 0;
  for (const char of body) {
    const value = base64Values.get(char);
    if (value === undefined) {
      return undefined;
    }
    buffer = (buffer << 6) | value;
    bufferedBits += 6;
    if (bufferedBits >= 8) {
      bufferedBits -= 8;
      decoded[decodedIndex] = (buffer >> bufferedBits) & 0xff;
      decodedIndex += 1;
    }
  }
  return decoded;
}
