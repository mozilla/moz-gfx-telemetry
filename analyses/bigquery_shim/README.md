# BigQuery shim for graphics-telemetry

## Installation

### pip

See this section on [VCS support in pip](https://pip.readthedocs.io/en/stable/reference/pip_install/#vcs-support).

```bash
pip install -e git+https://github.com/FirefoxGraphics/telemetry.git#egg=pkg&subdirectory=analyses/bigquery_shim
```

### Databricks

Run the included deploy script and update the notebook to include the
appropriate version of the shim.

```bash
./deploy.sh
```
