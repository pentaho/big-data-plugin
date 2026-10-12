# Changes Since `e72cecaf4fc38f94646bb253027d4744b414d2ef`

## Scope

This report covers the complete range from the requested baseline to the current
`automation_spike` tip:

- Baseline: `e72cecaf4fc38f94646bb253027d4744b414d2ef` (`Merge pull request #2998 from pentaho/abryant/PPP-6790-jsendnsca`)
- Baseline date: 2026-07-15
- Current tip: `85dd99b23574ade344d7c20a5291b30a678b200c` (`Fix of lock containers`)
- Current tip date: 2026-07-21
- Net result: 33 files changed, 4,683 insertions, no net deletions

Unlike the narrower comparison against `26b3c432...`, this range includes the original
automation work. It adds the Docker-backed integration-test module, test classes and
transformation fixtures, reusable GitHub Actions, pull-request and merge workflows, and
subsequent reliability fixes. The production Big Data plugin implementation is not changed
by this range; the additions are test, build, CI, and container orchestration infrastructure.

## Executive Summary

The branch introduces an end-to-end test pipeline for the Big Data plugin:

1. Maven can opt into a new `integration-test/pentaho-platform` module with
   `-DrunIntegrationTests`.
2. The module builds or downloads a plugin variant, creates a named-cluster metastore, and
   starts Hadoop, HBase, and a PDI client in a Docker network.
3. JUnit 5/Failsafe tests execute real `.ktr` transformations through `pan.sh` in the PDI
   container.
4. Tests verify both PDI output and backend state, using `hdfs dfs` and `hbase shell` through
   Docker CLI operations.
5. GitHub Actions runs the `apachevanilla` variant for pull requests and pushes to `master`
   and `release/*`, publishes Failsafe reports, and cleans Docker resources afterward.
6. The final commits make the setup usable on DinD/Kubernetes runners by embedding Hadoop
   assets in a derived image, using log-based readiness checks, and draining Docker process
   output concurrently.

## Commit Timeline

### `c06fe4c3c9a7f6873dda3498e154ce3484d99b79` - First approach and some tests

This is the main foundation commit. It adds 21 files and 2,538 lines covering the initial
integration-test module, Docker configuration, test harness, test classes, and documentation.

#### Build and module structure

- Adds the root `integration-test` module and `pentaho-platform` child module.
- Adds an opt-in root Maven profile activated by `-DrunIntegrationTests`, so ordinary builds
  do not start Docker containers or run integration tests.
- Configures Java 11 test compilation, JUnit Jupiter 5.10.2, AssertJ, Commons IO, Failsafe,
  Fabric8 Docker Maven Plugin, dependency unpacking, and Antrun directory setup.
- Adds local and download modes for the plugin under test:
  - `local` unpacks the locally installed plugin ZIP.
  - `download` resolves a published plugin variant through the Maven repository.
- Adds `apachevanilla` as the default test variant and supports the other assembly variants
  through matching Maven/profile properties.

#### Docker topology

The integration module provisions three containers on a custom network:

- Hadoop: `apache/hadoop:3.3.6`, exposed in the test network as `hadoop-hostname`.
- HBase: `harisekhon/hbase:2.1`, exposed as `hbase-hostname`.
- PDI client: the configured `automation/pdi-client` image, exposed as `pdi-hostname`.

Hadoop and HBase are started before PDI. The PDI image receives the plugin ZIP, named-cluster
metastore, Kettle configuration, and transformation fixtures through Docker assemblies. The
PDI container stays alive with `tail -f /dev/null` while tests execute commands into it.

The module also configures host-specific Docker endpoints:

- Windows: `tcp://localhost:2375`.
- Linux: `unix:///var/run/docker.sock`.
- macOS: `${env.DOCKER_HOST}`.

The default network is `bigdata-it-network`; `-Duse-existing-docker-network=<name>` can reuse
an existing network instead of creating one.

#### Hadoop configuration

- `core-site.xml` sets `fs.defaultFS` to the named Hadoop container and enables the root test
  user.
- `hdfs-site.xml` configures a single-node HDFS with replication factor 1, disabled HDFS
  permissions, fixed NameNode/DataNode directories, hostname-based DataNode access, and
  bind addresses suitable for container networking.
- `init-hdfs.sh` waits for HDFS and creates `/it/hdfs` and `/it/formats` with permissive test
  access. The script is provided as a helper; the Docker Maven Plugin does not automatically
  invoke it in the current POM.

#### Test harness and tests

The initial Java additions are described in detail in [Test Architecture](#test-architecture)
and [Tests](#tests).

#### Supporting documentation

Adds [`integration-test/plugin-integration-test-readme.md`](integration-test/plugin-integration-test-readme.md),
which documents the module layout, prerequisites, variant selection, run modes, and current
test status.

### `b9de7213df68b0773fa8d527d487884dbf23fac3` - Adding missing KTR fixtures

Adds six transformation fixtures required by the disabled backend suites:

- HBase input and output transformations.
- Parquet input and output transformations.
- ORC input and output transformations.

The `FormatsIT` class also declares Avro round-trip methods, but no Avro input/output KTR
files are present in the resulting tree. Since the whole class is currently `@Disabled`, this
missing fixture does not affect the active test run; it must be supplied or the Avro methods
removed before enabling `FormatsIT`.

### `d3970dcd960174eefc284025dae437cbd7f51aab` - Workflows

Adds the first CI implementation:

- [`plugin_pr.yaml`](.github/workflows/plugin_pr.yaml) invokes integration tests for pull
  requests.
- [`plugin_merge.yaml`](.github/workflows/plugin_merge.yaml) invokes them for merge pushes.
- [`integration_tests.yaml`](.github/workflows/integration_tests.yaml) is a reusable and
  manually dispatchable workflow with variant, runner, and branch inputs.
- [`setup-maven-settings/action.yml`](.github/actions/setup-maven-settings/action.yml) copies
  Maven settings and exposes repository credentials through environment variables.
- [`clean-docker/action.yml`](.github/actions/clean-docker/action.yml) removes stale Docker
  containers, networks, and volumes while attempting to preserve the CI job's own container.
- [`integration-test-summary/action.yml`](.github/actions/integration-test-summary/action.yml)
  parses Failsafe XML and writes a GitHub Step Summary, then uploads reports as an artifact.
- An initial `.github/settings.xml` supplies the Maven mirror configuration. That file is
  later removed when settings retrieval moves to `pentaho/actions-common`.

The initial reusable workflow checks out the requested ref, optionally frees disk space,
sets up Java, configures Maven and Docker registry credentials, cleans Docker, builds the
plugin, runs Failsafe, summarizes results, and cleans Docker again.

### `699f1ed60954f10dc99bddd5afcfa53ec2e9c15c` - Correct workflow branch triggers

- Changes merge workflow pushes from `main` to this repository's `master` branch.
- Removes the pull-request branch filter, allowing the workflow to run for pull requests
  regardless of their target branch while retaining path filters.

### `7e6097f94ae67a9d8a674c810b81020fe776348e` - Fix xmllint dependency

The workflow summary/version steps originally depended on `xmllint` being preinstalled. This
commit adds a best-effort installation step for `libxml2-utils` and `jq`, and replaces the
version extraction commands with portable `awk` and `sed` commands. The summary can degrade
gracefully if the tools cannot be installed.

### `e51ba3e90f5ad5238f43ade9f5a140ad5231a485` - Install Maven

Adds a workflow step that installs Maven through `apt-get` when `mvn` is not already on PATH,
then prints `mvn -version`. This supports runner images that provide Java but not Maven and
do not contain a Maven wrapper.

### `11b45cfa563979f287f4a63345ed6da98a19270d` - Change workflow Java version

Changes the workflow's `actions/setup-java` version from Java 11 to Java 17. The test module
still declares Java 11 compilation compatibility; the CI runtime uses Java 17.

### `c51f14284a0e45326ec4214943fb473e6b65b0f2` - Temporarily remove batch mode

Removes Maven's `-B` option from the build and integration-test commands while testing the
workflow. The later `60cdbc538c` commit restores batch mode and adds Docker log visibility.

### `30c7652199233ce604a64729e4abc72a6351922b2` - Change Maven repository naming

Changes the Maven mirror path used by the setup action from `maven` to `pnt-mvn`.

### `e87421207fb279546e011c2a1cca75b66b66cf0f` - Add x86_64 Linux container support

Adapts the CI workflow to the containerized x86_64 runner environment:

- Runs the job inside `${{ vars.PDIA_AC_CONTAINER_IMAGE }}` with Artifactory credentials.
- Mounts a shared Maven repository cache at `/m2_local_cache/`.
- Updates checkout to `actions/checkout@v7`.
- Removes the workflow's explicit Java and Maven installation steps because the job image
  provides the required tools.
- Switches Maven settings retrieval to `pentaho/actions-common@stable`.
- Uses `RESOLVE_REPO_MIRROR`, `NEXUS_DEPLOY_USER`, and `NEXUS_DEPLOY_PASSWORD` names expected
  by the shared settings file.
- Sets `MAVEN_ARGS` to use the shared local repository and retrieved settings file.
- Deletes the repository-local `.github/settings.xml`.

The branch's merge commit `26b3c43209a4f2765be0e504f59dbbfc48b31fcc` integrates this workflow
platform change and does not introduce an additional net file change in the corrected range.

### `60cdbc538c55e33f5a784f14802a7b55e9ffa087` - Logging to Docker container

Finalizes CI command behavior by:

- Restoring Maven batch mode (`-B`) for the plugin build.
- Restoring batch mode for the integration-test `verify` command.
- Adding `-Ddocker.showLogs=true` so Docker Maven Plugin container logs are visible in CI
  when startup or readiness fails.

### `026408debb32fbba4576f868b09cc17e36a0bc5a` - Fix Hadoop container

Changes the Hadoop test image from a base image with runtime host mounts to a derived image
named `bigdata-it-hadoop:${bigdata.it.pdi-version}`.

The derived image embeds:

- HDFS XML files at `/opt/hadoop/it-config`.
- Helper scripts at `/opt/hadoop/it-scripts`.

The old bind mounts are removed. This fixes runners where the Docker daemon cannot see the
Maven host filesystem, such as Docker-in-Docker or Kubernetes. Without the files, Hadoop
could start with its default `file:///` filesystem and the NameNode would fail.

The container command now copies the embedded configuration into Hadoop, formats the
NameNode when needed, starts the DataNode as a daemon, and runs the NameNode in the foreground.

### `aca618b5a0671eebf3071bd7d2b8b2d4b0aada3f` - Fix container validation

Replaces mapped TCP-port readiness checks with Docker-log readiness checks:

- Hadoop waits for `Processing first storage report`, which indicates DataNode registration
  and the first block report.
- HBase initially waits for `Master has completed initialization`.

Mapped host ports are not reliable when Maven runs outside the Docker network namespace even
though the tests communicate with sibling containers through Docker networking and `docker
exec`. Container logs remain available through the Docker API.

### `7b5904d16974b6958cbc4ee146d6909b3ad2397e` - Remove loose wait tags

Removes leftover `<wait>` and `<tcp>` opening tags from the previous POM edit, leaving one
valid log-based wait block per service container.

### `85dd99b23574ade344d7c20a5291b30a678b200c` - Fix container locks

This final commit has two reliability changes:

1. HBase readiness now waits for `service=ClientService` instead of the earlier HMaster
   initialization message. The `harisekhon/hbase` entrypoint follows daemon logs with `tail
   -f`, so an early initialization line can be missed. The client-service line is emitted
   later when the service is accepting requests.
2. `DockerUtils` drains stdout and stderr concurrently with two `CompletableFuture` readers.
   The previous sequential stream reads could deadlock when PAN or Docker output filled one
   OS pipe while Java was reading the other. The existing five-minute timeout, forced process
   destruction, and `ExecResult` API are retained.

## Test Architecture

### Maven activation and lifecycle

The root POM activates the integration-test module only when `runIntegrationTests` is set.
The integration parent then activates its `bigdata-it-profile`, which configures:

- `maven-failsafe-plugin` 3.2.5 with `integration-test` and `verify` goals.
- JUnit 5.10.2 and AssertJ test dependencies.
- One automatic rerun for a failing test (`bigdata.it.rerun-failing-tests-count=1`).
- Parallel execution disabled by default (`bigdata.it.parallel=false`).
- Fabric8 Docker Maven Plugin 0.48.1 with sequential container startup.
- Container build/start in `pre-integration-test` and container stop in
  `post-integration-test`.

The platform POM's preparation sequence is:

1. Create metastore, Kettle, and plugin-unpack directories.
2. Unpack the local plugin ZIP or download a published variant.
3. Generate the named-cluster metastore with the Docker service hostnames and ports.
4. Build/start Hadoop and HBase, waiting for readiness markers.
5. Build/start the PDI client after Hadoop and HBase.
6. Pass Docker container IDs to Failsafe as `bigdata.it.*-container-id` properties.
7. Run `*IT` classes through JUnit/Failsafe.
8. Stop the containers during post-integration-test cleanup.

### Test architecture classes

- [`BigDataPluginIT`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/BigDataPluginIT.java)
  is the base class. `runTransformation` invokes `pan.sh` inside the PDI container, checks
  the expected exit code, and checks required output substrings.
- [`DockerUtils`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/DockerUtils.java)
  wraps `docker exec`, `docker cp`, and shell pipelines. It returns `ExecResult` with exit code,
  stdout, stderr, and combined output.
- [`ITUtils`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/ITUtils.java)
  holds container/system-property constants and performs HDFS `-test`/`-cat` and HBase shell
  assertions through Docker.
- [`NamedClusterMetastoreBuilder`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/NamedClusterMetastoreBuilder.java)
  generates the metastore consumed by the PDI transformations.
- [`BigDataPluginCacheManager`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/BigDataPluginCacheManager.java)
  supports download mode for testing a published plugin variant.
- [`ExecutionType`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/ExecutionType.java)
  records the current PAN execution mode used by the test harness.

## Tests

The branch declares six JUnit test methods across four test classes. Two are active and four
are currently skipped because their containing classes are annotated with `@Disabled`.

### Active tests

| Class and method | Fixture | Behavior and assertions |
| --- | --- | --- |
| [`BasicIT.pluginLoadsAndTrivialTransformationRuns`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/BasicIT.java) | `basic/basic_smoke.ktr` | Runs a trivial transformation in PDI with PAN. Requires exit code 0, the `bigdata-it-smoke-ok` log marker, and no `ERROR` text. It verifies the PDI container and plugin bootstrap without touching a Hadoop backend. |
| [`HdfsIT.writeThenReadFromHdfs`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/HdfsIT.java) | `hdfs/hdfs_output.ktr`, `hdfs/hdfs_input.ktr` | Writes CSV data to `/it/hdfs/cars.csv` through the `hc://` VFS and named cluster, verifies the file exists with `hdfs dfs -test`, verifies `Toyota` and `Honda` with `hdfs dfs -cat`, then reads the file back through PDI and checks the `Toyota` result and PAN log markers. |

### Disabled tests

| Class and methods | Fixtures | Behavior when enabled | Current status |
| --- | --- | --- | --- |
| [`HBaseIT.writeThenReadFromHBase`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/HBaseIT.java) | `hbase/hbase_output.ktr`, `hbase/hbase_input.ktr` | Writes rows to table `it_cars` through HBase Output, scans it with `hbase shell`, checks `Toyota`, then reads it through HBase Input and checks the PDI result. | `@Disabled` pending live validation of fixtures and table bootstrap. A Failsafe selector does not override JUnit `@Disabled`. |
| [`FormatsIT.parquetRoundTrip`, `FormatsIT.avroRoundTrip`, `FormatsIT.orcRoundTrip`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/FormatsIT.java) | Parquet and ORC input/output fixtures exist. Avro fixtures are missing. | Each case writes a format file to `/it/formats` through `hc://`, verifies the HDFS path, reads it back through PDI, and checks format-specific log markers plus `Toyota`. | Whole class is `@Disabled` pending fixture validation. Supply Avro fixtures or remove that case before enabling. |

### Fixture inventory

- `basic/basic_smoke.ktr`: plugin-load smoke transformation.
- `hdfs/hdfs_output.ktr` and `hdfs/hdfs_input.ktr`: CSV HDFS write/read.
- `hbase/hbase_output.ktr` and `hbase/hbase_input.ktr`: HBase write/read.
- `formats/parquet_output.ktr` and `formats/parquet_input.ktr`: Parquet HDFS write/read.
- `formats/orc_output.ktr` and `formats/orc_input.ktr`: ORC HDFS write/read.
- Missing but referenced by `FormatsIT`: `formats/avro_output.ktr` and `formats/avro_input.ktr`.

## How to Run

### Prerequisites

- Java and Maven. The integration module compiles tests with Java 11; CI's containerized
  runner currently supplies the build toolchain.
- Docker with a running daemon.
- Access to the Pentaho Maven and Docker registries, normally through VPN and Artifactory
  credentials.
- The `automation/pdi-client:11.1` image or the configured equivalent.
- On Windows, Docker Desktop must expose `tcp://localhost:2375` because the POM's
  `windows-host` profile selects that endpoint. Override the Docker host property if the
  local Docker setup uses another endpoint.

### Local PowerShell run for `apachevanilla`

Run the integration test from the repository root so Maven can see the
`apachevanilla` assembly profile and build the plugin ZIP in the same reactor:

```powershell
mvn.cmd -pl integration-test/pentaho-platform -am clean verify -B -DrunIntegrationTests -Papachevanilla '-Dbigdata.it.plugin-variant=apachevanilla' '-Ddocker.showLogs=true'
```

This activates the integration-test module, produces the `apachevanilla` plugin ZIP before
the tests, creates the Docker network, builds/starts the containers, runs Failsafe, and stops
the containers. `-Ddocker.showLogs=true` preserves Docker startup diagnostics in the Maven
output. The dotted `-D` arguments are quoted because Windows PowerShell can split them when
they are passed through `mvn.cmd`.

If you prefer to build and install the plugin first, use this two-step alternative. The
module-local test command deliberately omits `-Papachevanilla`; that profile belongs to the
root assembly build and is not defined by the integration-test module:

```powershell
mvn.cmd clean install -B -Papachevanilla '-Dmaven.test.skip=true'

Push-Location integration-test/pentaho-platform
mvn.cmd verify -B -DrunIntegrationTests '-Dbigdata.it.plugin-variant=apachevanilla' '-Ddocker.showLogs=true'
Pop-Location
```

If the PDI image is hosted behind a registry mirror, add the corresponding prefix:

```powershell
mvn.cmd -pl integration-test/pentaho-platform -am clean verify -B -DrunIntegrationTests -Papachevanilla '-Dbigdata.it.plugin-variant=apachevanilla' '-Ddocker.showLogs=true' "-Dpentaho.docker.pull.host=$env:ARTIFACTORY_HOST/pnt-docker/"
```

The mirror command uses the same root-reactor form; only the Docker pull host changes.

### Run one active class

The Docker containers are still provisioned, but Failsafe runs only the selected class. Run
this from the repository root:

```powershell
mvn.cmd -pl integration-test/pentaho-platform -am clean verify -B -DrunIntegrationTests -Papachevanilla '-Dbigdata.it.plugin-variant=apachevanilla' '-Dit.test=BasicIT' '-Ddocker.showLogs=true'
```

Replace `BasicIT` with `HdfsIT` for the HDFS round trip. Use `-Dit.test`, not `-Dtest`, because
these classes are executed by Maven Failsafe during the integration-test lifecycle.

### Test another plugin variant

Set the variant to one whose shim artifact is available in your Maven repositories. The final
variant plugin ZIP is assembled by this project, while the distribution-specific shim must be
available as a published Maven dependency for every variant, including `apachevanilla`. The build
profile and classifier must match:

```powershell
$variant = 'dataproc'

mvn.cmd -pl integration-test/pentaho-platform -am clean verify -B -DrunIntegrationTests "-P$variant" "-Dbigdata.it.plugin-variant=$variant" '-Ddocker.showLogs=true'
```

Supported variants in the documented assembly setup include `apachevanilla`, `cdp`, `hdi`,
`emr`, and `dataproc`. All of their final plugin ZIPs are assembled in this repository, but every
distribution-specific shim is an external published artifact and requires Artifactory access.

### Test a published variant

Download mode skips local plugin assembly and downloads a complete published plugin ZIP through
`BigDataPluginCacheManager`:

```powershell
$variant = 'dataproc'

Push-Location integration-test/pentaho-platform
mvn.cmd verify -B -DrunIntegrationTests '-Dbigdata.it.source=download' "-Dbigdata.it.plugin-variant=$variant" '-Ddocker.showLogs=true'
Pop-Location
```

This mode uses the `download` profile in the integration-test POM, so no local plugin assembly
profile or reactor build is required. For PDI `11.1`, the default source is the latest QAT artifact
at `https://build.eng.pentaho.com/hosted/11.1-QAT/latest/` with the filename
`pentaho-big-data-ee-plugin-<variant>.zip`. Use `-Dbigdata.it.plugin.url=<url>` for another
published ZIP. The requested variant must exist at that URL.

### CI execution

The GitHub Actions entry points are:

- [`plugin_pr.yaml`](.github/workflows/plugin_pr.yaml): pull requests, with superseded runs
  cancelled.
- [`plugin_merge.yaml`](.github/workflows/plugin_merge.yaml): pushes to `master` and
  `release/*`, without cancelling an in-progress merge run.
- [`integration_tests.yaml`](.github/workflows/integration_tests.yaml): reusable workflow and
  manual `workflow_dispatch` entry point with `runs_on`, `plugin_variant`, and `branch` inputs.

The reusable workflow performs this sequence:

1. Runs inside the configured x86_64 PDI automation container and mounts a shared Maven cache.
2. Checks out the requested branch.
3. Installs `xmllint`/`jq` best-effort for report parsing when available.
4. Resolves plugin and PDI versions from the POM files.
5. Retrieves Maven settings and logs in to the Artifactory Docker registry.
6. Removes stale Docker resources.
7. Builds the selected plugin variant in Maven batch mode.
8. Runs the integration tests with Docker logs enabled.
9. Parses and uploads Failsafe results, even when tests fail.
10. Performs post-build Docker cleanup.

## Reports and Diagnostics

Local Failsafe results are written to:

`integration-test/pentaho-platform/target/failsafe-reports`

Important files include `failsafe-summary.xml`, per-class `TEST-*.xml` files, and text reports.
The CI summary action extracts completed, failure, error, skipped, and flaky counts; adds
failure/error/skipped details and stack traces to the GitHub Step Summary; and uploads the
report directory as a 30-day artifact.

For container failures, inspect the Maven output with `-Ddocker.showLogs=true`. The most
important startup readiness strings are:

- Hadoop: `Processing first storage report`.
- HBase: `service=ClientService`.

## Current Limitations

- Only `BasicIT` and `HdfsIT` are active. HBase and format tests are intentionally disabled.
- `FormatsIT` references missing Avro fixtures.
- Readiness depends on exact upstream log text from the selected Hadoop/HBase images.
- Docker process commands have a five-minute timeout in `DockerUtils`.
- The default test setup is sequential because HDFS/HBase state is shared. Parallel execution
  can be enabled with `-Dbigdata.it.parallel=true`, but the tests are not written with isolated
  backend paths/tables for concurrent execution.
- The active workflow runs `apachevanilla`; other plugin variants are not automatically covered
  by PR/merge CI unless the reusable workflow is dispatched with another `plugin_variant`.
- The Windows Docker profile assumes Docker Desktop's unauthenticated TCP endpoint unless
  overridden.

## Files Added or Modified in the Corrected Range

### CI and GitHub Actions

- [`.github/workflows/plugin_pr.yaml`](.github/workflows/plugin_pr.yaml)
- [`.github/workflows/plugin_merge.yaml`](.github/workflows/plugin_merge.yaml)
- [`.github/workflows/integration_tests.yaml`](.github/workflows/integration_tests.yaml)
- [`.github/actions/setup-maven-settings/action.yml`](.github/actions/setup-maven-settings/action.yml)
- [`.github/actions/clean-docker/action.yml`](.github/actions/clean-docker/action.yml)
- [`.github/actions/integration-test-summary/action.yml`](.github/actions/integration-test-summary/action.yml)

### Integration-test implementation

- [`integration-test/pom.xml`](integration-test/pom.xml)
- [`integration-test/pentaho-platform/pom.xml`](integration-test/pentaho-platform/pom.xml)
- [`integration-test/pentaho-platform/docker/hadoop/config/core-site.xml`](integration-test/pentaho-platform/docker/hadoop/config/core-site.xml)
- [`integration-test/pentaho-platform/docker/hadoop/config/hdfs-site.xml`](integration-test/pentaho-platform/docker/hadoop/config/hdfs-site.xml)
- [`integration-test/pentaho-platform/docker/hadoop/scripts/init-hdfs.sh`](integration-test/pentaho-platform/docker/hadoop/scripts/init-hdfs.sh)
- [`integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/BasicIT.java`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/BasicIT.java)
- [`integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/BigDataPluginIT.java`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/BigDataPluginIT.java)
- [`integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/DockerUtils.java`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/DockerUtils.java)
- [`integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/ITUtils.java`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/ITUtils.java)
- [`integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/NamedClusterMetastoreBuilder.java`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/NamedClusterMetastoreBuilder.java)
- [`integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/BigDataPluginCacheManager.java`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/BigDataPluginCacheManager.java)
- [`integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/ExecutionType.java`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/ExecutionType.java)
- [`integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/HBaseIT.java`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/HBaseIT.java)
- [`integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/HdfsIT.java`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/HdfsIT.java)
- [`integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/FormatsIT.java`](integration-test/pentaho-platform/src/test/java/org/pentaho/big/data/it/FormatsIT.java)
- `integration-test/pentaho-platform/src/test/resources/transformations/**`

The root [`pom.xml`](pom.xml) enables the integration-test module through the opt-in profile,
and [`.gitignore`](.gitignore) ignores the generated plugin unpack directory.