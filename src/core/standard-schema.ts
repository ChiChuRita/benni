// Minimal inline Standard Schema v1 (https://standardschema.dev) — the
// ecosystem-wide validator interface implemented by Zod, Valibot, ArkType,
// and friends. Inlined so benni stays zero-dependency.

export interface StandardSchemaV1<Input = unknown, Output = Input> {
  readonly "~standard": {
    readonly version: 1;
    readonly vendor: string;
    readonly validate: (
      value: unknown
    ) => StandardSchemaResult<Output> | Promise<StandardSchemaResult<Output>>;
    readonly types?:
      | { readonly input: Input; readonly output: Output }
      | undefined;
  };
}

export type StandardSchemaIssue = {
  readonly message: string;
  readonly path?:
    | ReadonlyArray<PropertyKey | { readonly key: PropertyKey }>
    | undefined;
};

export type StandardSchemaResult<Output> =
  | { readonly value: Output; readonly issues?: undefined }
  | { readonly issues: readonly StandardSchemaIssue[] };

/** The validator's output type: what `json(schema)` decodes to. */
export type InferStandardOutput<S extends StandardSchemaV1> = NonNullable<
  S["~standard"]["types"]
>["output"];

/** The validator's input type: what `json(schema)` accepts on write. */
export type InferStandardInput<S extends StandardSchemaV1> = NonNullable<
  S["~standard"]["types"]
>["input"];

export function formatStandardIssues(
  issues: readonly StandardSchemaIssue[]
): string {
  const shown = issues.slice(0, 3).map((issue) => {
    const path = issue.path
      ?.map((part) =>
        typeof part === "object" ? String(part.key) : String(part)
      )
      .join(".");
    return path ? `${path}: ${issue.message}` : issue.message;
  });
  const more = issues.length > 3 ? ` (+${issues.length - 3} more)` : "";
  return shown.join("; ") + more;
}
