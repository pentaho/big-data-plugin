# Big Data plugin integration tests

The opt-in integration suite builds and installs the Big Data plugin into a disposable PDI CE
client container (`edition: "ce"`) managed by `penv`, alongside disposable Hadoop/YARN, seeded
PostgreSQL, and HBase containers defined in `integration-test/pentaho-platform/penv/compose.yaml`.

`SqoopIT` imports rows from PostgreSQL into HDFS through Kitchen and asserts both the Sqoop
MapReduce counters in the Kitchen log and the landed HDFS files. `BasicIT` and `HdfsIT` also run
as part of the enabled suite; unfinished `FormatsIT` and `HBaseIT` cases remain `@Disabled`.

- **Architecture & code walkthrough (with Mermaid diagrams):**
  [penv-integration-architecture.md](penv-integration-architecture.md)
- **Sqoop, Hadoop, PDI & Java 21 bytecode compatibility notes:**
  [sqoop-hadoop-java-versions.md](sqoop-hadoop-java-versions.md)

## Prerequisites

- JDK 17 or later on the host (`penv` runs PDI and Hadoop on Java 21 inside Docker) and Maven 3.8+.
- Python 3.8+ (`python3` on Linux/macOS, `python` or `py -3` on Windows).
- Docker with Compose v2 (`docker compose`, 2.24.4+) and BuildKit, running Linux `amd64`
  containers (use Docker Desktop in **Linux containers** mode on Windows; WSL is not required).
- Network access to Pentaho's Maven repository and internal artifact cache
  (`repo-cache.eng.pentaho.com`, used by `penv` to cache the PDI CE client distribution).
- `penv` installed from `pentaho/eng-sandbox` (pinned in `.github/workflows/integration_tests.yaml`
  at commit `7326f7253759a6eafa98b4fff4fa46ad7a397c12`).
- No PDI license file is required for the default PDI CE setup (`edition: "ce"`). If you switch
  `penv/penv.json` to `"edition": "ee"`, supply `-Dbigdata.it.license-file=/path/to/.elmLicInfo.plt`
  (never commit the license file).

## Install `penv`

Install `penv` once on your workstation from a local `pentaho/eng-sandbox` checkout:

**Linux / macOS (Bash)**

```sh
bash /path/to/eng-sandbox/penv/install.sh --local
```

**Windows (PowerShell 7 `pwsh` or Windows PowerShell 5.1)**

```powershell
# Invoking via UTF-8 text avoids Windows PowerShell 5.1's non-BOM UTF-8 parser quirk:
$installer = Join-Path 'D:\pentaho-projects\eng-sandbox\penv' 'install.ps1'
& ([scriptblock]::Create([System.IO.File]::ReadAllText($installer, [System.Text.Encoding]::UTF8))) -Local
```

Maven discovers `penv` automatically from `PATH`, `~/.local/bin/penv` (Unix), or
`%LOCALAPPDATA%\penv\bin\penv.cmd` (Windows). You can also point explicitly to a binary or
`penv_runtime.py` via `-Dbigdata.it.penv-bin=<path>`.

## Run locally with Maven

Maven orchestrates the full lifecycle on both Linux and native Windows with zero host shell
scripts:

1. Unpacks the built `pentaho-big-data-plugin` variant ZIP and generates the Named Cluster
   metastore XML (`process-test-classes`).
2. Stages an isolated `penv` workspace under `target/penv/<timestamp>` and runs `penv start pdi`
   to build and wait for PDI, Hadoop/YARN, PostgreSQL, and HBase (`pre-integration-test`).
3. Runs JUnit 5 / Failsafe tests against the live containers (`integration-test`).
4. Runs `penv destroy --yes` to remove all containers, networks, volumes, and per-run fixture
   images (`post-integration-test`), then fails the build if any test failed (`verify`).

**Linux (Bash)**

```sh
mvn clean install -B -Papachevanilla -Dmaven.test.skip=true &&
  mvn -f integration-test/pentaho-platform/pom.xml verify -B -DrunIntegrationTests \
    -Papachevanilla -Dit.test=SqoopIT
```

**Windows (PowerShell)**

```powershell
mvn clean install -B -Papachevanilla '-Dmaven.test.skip=true'
if ($LASTEXITCODE -eq 0) {
  mvn -f integration-test\pentaho-platform\pom.xml verify -B -DrunIntegrationTests `
    -Papachevanilla '-Dit.test=SqoopIT'
}
```

> **PowerShell quoting note:** Always quote `-D` properties containing dots (such as
> `'-Dit.test=SqoopIT'`), because PowerShell splits unquoted `-Dfoo.bar=baz` arguments at `.`.

Omit `-Dit.test=SqoopIT` to run all enabled integration tests (`BasicIT`, `HdfsIT`, `SqoopIT`).
Do not pass `-DskipTests` on `verify`, as Failsafe also honors it; inspect
`integration-test/pentaho-platform/target/failsafe-reports` to verify execution counts.

## Workspace visibility & CI configuration

- **License handling:** PDI CE (`edition: "ce"`) requires no license. When testing against PDI EE
  (`edition: "ee"`), pass `-Dbigdata.it.license-file=/path/to/.elmLicInfo.plt`; the license is
  copied into the running PDI container before each test class and deleted in `@AfterAll`, and is
  never baked into an image or `penv` workspace.
- **Bind-mount path visibility:** `penv` bind-mounts `<workspace>/data` into the PDI container,
  so the Docker daemon must see the workspace directory at the same path as the Maven process.
  `PenvEnvironmentManager` verifies this before building images. On containerized or remote-daemon
  runners where only a specific directory is shared with Docker, set `-Dbigdata.it.penv-workspace-root=<path>`
  or the `BIGDATA_IT_WORKSPACE_ROOT` environment/Actions variable.
- **GitHub Actions secrets & variables:** `.github/workflows/integration_tests.yaml` uses the
  organization `HV_ACTIONS_GIT_TOKEN` secret (or an optional `BIGDATA_IT_PENV_REPO_TOKEN`
  override) for read-only checkout of `pentaho/eng-sandbox`, plus standard Artifactory credentials.
  `BIGDATA_IT_LICENSE_INFORMATION` is only needed if testing PDI EE. Optional repository variables
  `BIGDATA_IT_WORKSPACE_ROOT` and `BIGDATA_IT_PENV_HOME` customize the shared workspace root and
  persistent `penv` download cache directory on self-hosted runners.
