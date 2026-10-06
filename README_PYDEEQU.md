# PyDeequ Data Quality Test Suite

Data-quality validation for the ADP star-schema tables (Danone / Sauersbrands / Kind),
built on [PyDeequ](https://github.com/awslabs/python-deequ) + pytest + Allure, running
on Databricks.

- Test implementation: [`tests/test_pydeequ_implementation.py`](tests/test_pydeequ_implementation.py)
- Databricks runner notebook: `New Notebook 2026-04-21 16_49_27.ipynb`
- Dataset registry: [`test_data/dataset_config.py`](test_data/dataset_config.py)
- Brand/dataset filtering: [`utils/config_helper.py`](utils/config_helper.py)

---

## 1. Architecture

| Component | Value |
|---|---|
| Compute | `asper-pydeequ-compute` (Databricks all-purpose cluster) |
| Databricks Runtime | 15.4 LTS (Spark 3.5.0, Scala 2.12) |
| Worker type | `Standard_D4ds_v5`, autoscaling 1–8 workers |
| Cluster library | `com.amazon.deequ:deequ:2.0.8-spark-3.2` (Maven, installed on the cluster) |
| Data access | Unity Catalog |
| Python package | `pydeequ==1.3.0` (installed via notebook `%pip`, not a cluster library) |

**Important mismatch to resolve:** the cluster has the Deequ 2.0.8 JAR built for Spark 3.2
attached, but the compute runs Spark 3.5.0. PyDeequ's Python wrapper also sets
`SPARK_VERSION=3.1` via `%env` in the notebook. This "works" because PyDeequ maps
`SPARK_VERSION` to a Maven coordinate it resolves itself unless the JAR is already on
the classpath — but having both a manually-attached cluster library *and* PyDeequ's own
resolution path is fragile and should be collapsed into one source of truth (see
[Scope → Risks](#4-known-gaps--risks) below).

---

## 2. Setup

### 2.1 One-time cluster setup
1. Create/attach a Databricks cluster on **Runtime 15.4 LTS** (or any Spark 3.x runtime
   PyDeequ supports).
2. Either:
   - **Option A (cluster-level, current state):** attach `com.amazon.deequ:deequ:2.0.8-spark-3.2`
     as a Maven library on the cluster (Compute → Libraries → Install new), **or**
   - **Option B (notebook-level, recommended):** remove the cluster library and let PyDeequ
     resolve the matching Deequ JAR itself via `SPARK_VERSION`, so the dependency travels
     with the notebook/repo instead of a manually-configured cluster.
3. Grant the cluster's identity read access to the Unity Catalog schemas under test
   (`asper_production_*_catalog.silver_us.*`) and to the Volume holding the ADP schema
   workbook (`/Volumes/.../adp_silver_schema/...xlsx`).

### 2.2 Per-session setup (notebook cells)
Run in order, each followed by `%restart_python` where shown:

```python
%pip install pydeequ==1.3.0
%restart_python
```
```python
# Spark 3.1/3.2/3.3-compatible Deequ resolution
%env SPARK_VERSION=3.1
```
```python
!pip install pytest allure-pytest openpyxl
```
```python
# one-time per cluster: fetch the Allure CLI (report generator) locally
import os
os.system("""
cd /tmp &&
wget https://github.com/allure-framework/allure2/releases/download/2.27.0/allure-2.27.0.tgz &&
tar -xvzf allure-2.27.0.tgz &&
mv allure-2.27.0 allure
""")
```

### 2.3 Run the suite
```python
import sys, pytest
sys.path.insert(0, "/Workspace/Users/<you>/dataplatform_automation_test/etl_test/")

exit_code = pytest.main([
    "tests/test_pydeequ_implementation.py",
    "-sv",
    "--alluredir=/tmp/allure-results",
])
```

### 2.4 Generate & publish the Allure report
```python
import subprocess
subprocess.run([
    "/tmp/allure/bin/allure", "generate",
    "/tmp/allure-results", "-o", "/tmp/allure-report",
    "--clean", "--single-file",
]).check_returncode()

import shutil
shutil.copy("/tmp/allure-report/index.html", "<workspace_or_volume_path>/Data_Validation_Report.html")
```

### 2.5 Local (non-Databricks) setup
```bash
pip install -r etl_test/tests/requirements.txt
pytest etl_test/tests/test_pydeequ_implementation.py --brand=ALL --dataset=ALL --alluredir=allure-results
allure generate allure-results -o allure-report --clean
```

### CLI options
| Flag | Default | Example |
|---|---|---|
| `--brand` | `ALL` | `--brand=Danone`, `--brand=Danone,Kind` |
| `--dataset` | `ALL` | `--dataset=geo_dim` |

Datasets are resolved from `DATASETS` in [`test_data/dataset_config.py`](test_data/dataset_config.py)
via `get_all_datasets()`.

---

## 3. Scope — what's covered today

Each check runs per `(customer, dataset)` pair, parametrized from `DATASETS`.

**Hard assertions (fail the build):**
- Record count > 0 (`Size`)
- Schema contract vs. the ADP schema sheet (missing/unexpected columns) — only when a
  `sheet_name` is configured
- Composite-key duplicate count == 0 (`CountDistinct` − `Uniqueness` derivation)
- Composite-key NULL count == 0, with configurable ignore-values (`"None"`, `"NA"`, `"N/A"`)
- Key-column completeness == 100%
- Numeric key columns are non-negative (native `VerificationSuite` constraint)
- `is_current` flag is `true` for every row, when that column exists
- ADP key-contract check: configured keys must exist in the ADP schema sheet **and** be
  100% complete

**Warn-only profiling (always pass, surfaced in Allure for visibility):**
- Null counts for every column
- Per-column completeness profile (flags fully-empty columns)
- ADP-expected-column completeness/distinctness
- Combined composite-key uniqueness ratio
- Per-key distinctness
- Categorical entropy on string columns
- Date-pattern match ratio (`YYYY-MM-DD`) on date-like string columns
- Numeric summary (mean/stddev/min/max/sum) and standard deviation
- Full dataset profile and single-column deep profile

All checks are derived **purely from PyDeequ analyzer/verification output** (not from
plain Spark SQL), so the metrics match what Deequ would report natively — see the module
docstrings in `test_pydeequ_implementation.py` for the exact parity notes (e.g. NULL
handling differences between `Uniqueness` and `CountDistinct`).

Results are logged to the console and attached to **Allure** (HTML tables for summaries,
text attachments for scalar results), tagged with `feature=<customer>` / `story=<dataset>`.

---

## 4. Known gaps / risks

- **JAR/runtime version drift**: cluster-level Deequ 2.0.8-spark-3.2 JAR vs. Spark 3.5.0
  runtime vs. notebook's `SPARK_VERSION=3.1` — pick one resolution path and document it.
- **Version pinning is inconsistent**: notebook installs `pydeequ==1.3.0`, but
  `tests/requirements.txt` pins `pydeequ==1.1.0`. Align these.
- **Hardcoded personal paths**: the runner notebook hardcodes
  `/Workspace/Users/atul.gupta2@asper.ai/...`. Any other user running it must edit the
  notebook. Should come from a Databricks widget, job parameter, or repo-relative path.
- **Allure tarball downloaded on every run** and not cached/pinned to a Workspace Volume —
  slow and a repeated external-network dependency inside a (presumably) controlled
  Databricks workspace.
- **PyDeequ reports metrics, not offending rows.** Every failing check (duplicates, key
  nulls, non-negative) tells you *how many* rows are bad, not *which* ones — there's no
  quarantine/sample of failing records attached to the Allure report today.
- **`DATASETS` in `dataset_config.py` is mostly commented out** — only `Sauersbrands.geo_dim`
  and `Sauersbrands.time_dim` are currently live; Danone/Kind entries exist but are disabled.
- No cross-table checks (referential integrity between fact and dimension tables,
  e.g. every `GEOGRAPHY_KEY` in a fact table exists in `star_geo_dim`).

---

## 5. Suggested features to add next

1. **CI/CD trigger** — wire this suite into a Databricks Job (or GitHub Actions calling
   the Jobs API) so it runs on a schedule or on PR against the ETL pipeline repo, instead
   of being run manually from a notebook.
2. **Referential integrity checks** — Deequ supports this indirectly via `Compliance`
   with a subquery/join predicate; add fact→dimension foreign-key checks (e.g. every
   `DATE_KEY` in a fact table exists in `star_time_dim`).
3. **Metrics history / drift detection** — persist `AnalyzerContext.successMetricsAsDataFrame`
   output to a Delta table per run (with run timestamp), then compare against a rolling
   baseline to catch gradual drift (e.g. completeness slowly degrading) rather than only
   hard thresholds.
4. **Anomaly detection** — PyDeequ ships `AnomalyDetectingRunner` / metrics-repository
   support for exactly this; currently unused here.
5. **Failing-row sampling** — alongside the aggregate metric, run a cheap follow-up
   `df.filter(...).limit(N)` only when a check fails, and attach the sample to Allure for
   faster triage (duplicates, null keys, non-current rows).
6. **Slack/Teams notification on failure** — post a summary (brand, dataset, failed
   checks) to a webhook after the pytest run, instead of relying on someone opening the
   Allure HTML.
7. **Config validation test** — a small test that asserts every entry in `DATASETS` has
   a resolvable `table` and (if present) a valid `sheet_name`, so a typo in
   `dataset_config.py` fails fast instead of at fixture time with an opaque error.
8. **Parameterize the runner notebook** — replace the hardcoded `/Workspace/Users/...`
   path with `dbutils.widgets.get(...)` or a job parameter, so the same notebook works
   for any user/environment without editing.
9. **Multi-environment support** — parameterize the catalog (`asper_production_*` vs.
   a dev/staging catalog) so the same suite can run against lower environments before
   promoting ETL changes to production.
10. **Parallelize dataset runs** — `pytest-xdist` (already in `requirements.txt` but
    unused) to run independent `(customer, dataset)` cases concurrently and cut wall-clock
    time as more datasets are enabled.
11. **Secrets hygiene** — the (unrelated, commented-out) `conftest.py` block has a blank
    `storage_key` variable; if storage-account keys are ever needed again, source them
    from Databricks secret scopes, never inline.

---

## 6. File reference

| Path | Purpose |
|---|---|
| `tests/test_pydeequ_implementation.py` | All PyDeequ analyzers/checks + pytest test classes |
| `tests/pydeequ_jvm.py` | Earlier/alternate JVM-based PyDeequ analyzer implementation |
| `tests/requirements.txt` | Pinned dependencies for local/CI runs |
| `conftest.py` | `--brand` / `--dataset` CLI options, `input_data` fixture |
| `test_data/dataset_config.py` | `DATASETS` registry: table path, ADP sheet name, composite keys |
| `utils/config_helper.py` | Brand/dataset filtering (`get_all_datasets`), `has_schema_sheet` |
| `utils/validations.py` | Non-PyDeequ (plain Spark) validation helpers used for schema diffing |
| `utils/data_loader.py` | Loads the table + ADP schema sheet into `input_data` |
