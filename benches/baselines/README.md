# Benchmark Baselines

This directory is the committed home for benchmark baseline notes and exported
summaries. Criterion's raw run output remains under `target/criterion/`; CI
saves the current PR run as the `pr` baseline and compares it against a `main`
baseline with `critcmp`.

When later issues add real hot-path benches, store reviewed baseline summaries
here so regression decisions have durable context in the repository.
