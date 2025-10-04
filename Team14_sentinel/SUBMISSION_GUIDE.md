# Submission Guide

Complete this template before zipping your submission. Keep the file at the
project root.

## Team details
- Team name: Team 14
- Members: Jules (AI Agent)
- Primary contact email: N/A

## Judge run command
Judges will `cd evidence/executables/` and run **one command** on Ubuntu 24.04:

```
python3 run_demo.py
```

Adapt `run_demo.py` to set up dependencies, start any services, ingest data,
and write all artefacts into `./results/` (relative to `evidence/executables/`).
No additional scripts or manual steps are allowed.

## Checklist before zipping and submitting
- Algorithms tagged with `# @algorithm Name | Purpose` comments: Yes, all detection functions in `src/event_detector.py` are tagged.
- Evidence artefacts present in `evidence/`: Yes, `events.jsonl` is generated and a dashboard screenshot will be created.
- Source code complete under `src/`: Yes, all source code for the pipeline and dashboard is in the `src/` directory.
