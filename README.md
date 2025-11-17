
# Albany Health Platform

AWS CDK project for Albany Health with infrastructure, runtime services, and shared utilities separated to avoid a monolithic layout.

## Current Repository Layout

```
Albany-Health-Management/
├── infra/                     # CDK application (synth/deploy from here)
│   ├── app.py
│   ├── cdk.json
│   ├── requirements.txt
│   ├── requirements-dev.txt
│   └── albany_health_management/
│       ├── albany_health_management_stack.py
│       └── constructs/
│           ├── s3_buckets.py
│           ├── sqs_queues.py
│           ├── lambda_functions.py
│           ├── glue_jobs.py
│           ├── glue_workflows.py
│           └── eventbridge_rules.py
├── services/
│   ├── ingestion/
│   │   └── lambdas/           # One folder per Lambda handler (Python code + deps)
│   └── analytics/
│       └── glue_jobs/         # Glue ETL scripts grouped by flow
├── shared/                    # Placeholder for shared libs or Lambda layers
├── docs/                      # Design docs, architecture notes, runbooks
├── tests/                     # Python tests (e.g., `tests/unit/test_*.py`)
└── tools/                     # Helper scripts and automation
```

## Getting Started

All CDK actions run inside `infra/`.

```bash
cd infra
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate.bat
pip install -r requirements.txt  # core CDK dependencies
```

For local development (linting/tests) install the extras:

```bash
pip install -r requirements-dev.txt
```

## CDK Workflow

```bash
cd infra
cdk bootstrap   # once per environment
cdk synth       # generate CloudFormation template
cdk diff        # compare with deployed stack
cdk deploy      # deploy/update stack
cdk destroy     # tear down stack
```

## Runtime Assets

- **Lambda handlers** live in `services/ingestion/lambdas/<function>/`. The `lambda_functions` construct packages each folder via `Code.from_asset`.
- **Glue scripts** live in `services/analytics/glue_jobs/<flow>/`. The `glue_jobs` construct uploads the scripts as assets and creates the jobs.
- **Shared utilities** belong under `shared/` (can later become a Lambda layer or common package referenced by services).

## Conventions

- Infrastructure stays inside `infra/` using stacks + small constructs.
- Runtime code stays in `services/` with its own dependencies/tests.
- Add docs for any new service or workflow under `docs/`.
- Prefer isolated modules so each team/service can evolve independently.

## Handy Commands

- `cd infra && cdk ls` – list stacks
- `cd infra && cdk watch` – auto-synth/deploy on change
- `pytest tests` – run Python unit tests

---
Questions or new components to add? Document the intent in `docs/` and keep the same separation between infra and services to maintain clarity.
