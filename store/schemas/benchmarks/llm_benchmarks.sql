-- Delta Lake Schema: llm_benchmarks
-- In-process LLM benchmark evaluations and inference telemetry (AD-22).
-- Invariant: Zero PII, microsecond latencies, token throughput, and safety invariants.

CREATE TABLE IF NOT EXISTS llm_benchmarks (
    run_id STRING NOT NULL,                      -- UUIDv7 benchmark evaluation run identifier
    model_id STRING NOT NULL,                    -- Model identifier (e.g., gemma-2-9b-it, claude-3-5-sonnet)
    provider STRING NOT NULL,                    -- Provider name (e.g., ollama, deepseek, anthropic)
    ttft_us BIGINT NOT NULL,                     -- Time-To-First-Token in microseconds
    tps DOUBLE NOT NULL,                         -- Generated tokens per second
    itl_jitter_us DOUBLE NOT NULL,               -- Inter-Token Latency jitter in microseconds
    tool_call_valid BOOLEAN NOT NULL,            -- Verification boolean for tool invocation schema correctness
    domain_invariant_valid BOOLEAN NOT NULL,     -- Domain invariant checks passed (e.g., no negative credit)
    pii_masked BOOLEAN NOT NULL,                 -- Guardrail check confirming zero PII in generated output
    timestamp_utc TIMESTAMP NOT NULL             -- Microsecond precision UTC benchmark timestamp
)
USING DELTA
PARTITIONED BY (model_id)
LOCATION 'data/delta/llm_benchmarks'
TBLPROPERTIES (
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '2'
);
