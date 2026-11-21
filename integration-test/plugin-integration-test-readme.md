# Pentaho Big Data Plugin — Integration Tests

Functional integration tests for the `pentaho-big-data-plugin`. Each test runs a real PDI
transformation (via `pan.sh`) inside a `pdi-client` Docker container that has the big-data plugin
installed, against real Hadoop backends (HDFS, HBase and Formats over HDFS) running as sibling
Docker containers. Assertions check **both** the backend state (files in HDFS, rows in HBase) and
the **captured PDI logs**.

This module mirrors the OpenLineage Plugin (OLP) integration-test infrastructure, adapted for the
CE big-data repository (self-contained Docker helper based on the `docker` CLI, no EE
`automation-utils` dependency).

## Layout

```
integration-test/
  pom.xml                              parent pom (docker network, plugin management, host profiles)
  pentaho-platform/
    pom.xml                            main IT module (plugin download/unpack, containers, failsafe)
    docker/
      hadoop/config/                   core-site.xml / hdfs-site.xml for the Hadoop container
      hadoop/scripts/init-hdfs.sh      helper to create /it base dirs in HDFS
    src/test/java/org/pentaho/big/data/it/
      DockerUtils.java                 docker exec / cp wrapper (ProcessBuilder)
      ITUtils.java                     paths, system-property keys, HDFS/HBase helpers
      BigDataPluginIT.java             base class: runTransformation(...) via pan.sh
      BigDataPluginCacheManager.java   downloads a published plugin variant (nightly mode)
      NamedClusterMetastoreBuilder.java generates the named-cluster metastore
      ExecutionType.java               PAN
      BasicIT.java                     smoke test
      HdfsIT.java                      HDFS write/read round trip
      HBaseIT.java                     HBase write/read round trip (@Disabled, pending fixtures)
      FormatsIT.java                   Parquet/Avro/ORC round trips (@Disabled, pending fixtures)
    src/test/resources/transformations/
      basic/basic_smoke.ktr
      hdfs/hdfs_output.ktr, hdfs/hdfs_input.ktr
```

## Requirements

- Docker available on the host (daemon running).
- Network access to the Pentaho Docker registry
  (`pntprv-docker-dev-orl.repo.eng.pentaho.com`) — VPN + `docker login` required to pull the
  `automation/pdi-client` image.
- On **Windows**, expose the Docker daemon on `tcp://localhost:2375` (Docker Desktop:
  *Settings → General → Expose daemon on tcp://localhost:2375 without TLS*). The `windows-host`
  profile sets `bigdata.it.dockerHost` accordingly. Linux uses the unix socket, macOS uses the
  `DOCKER_HOST` env var.

## Running

Integration tests are **off by default**. They are enabled with the `runIntegrationTests` property,
and require a plugin assembly variant to be built (the plugin `zip` artifact is only produced when
one of the assembly variant profiles is active — see below).

### Build the project first

Build and install the whole project (including the plugin `apachevanilla` zip) into the local Maven
repository before running the integration tests:

```
mvn clean install -Papachevanilla -Dmaven.test.skip=true
```

> Do **not** use `-am clean` on the integration-test commands below — the project is already built
> and installed by the command above, so the IT run only resolves the already-installed artifacts.

### PR / merge (build the plugin locally and test it)

```
mvn -pl integration-test/pentaho-platform verify -DrunIntegrationTests -Papachevanilla
```

This unpacks the previously built `pentaho-big-data-plugin` `apachevanilla` zip into the
`pdi-client` container and runs the tests.

#### Building a variant other than `apachevanilla`

##### How the plugin is assembled (and why testing several shims matters)

The `pentaho-big-data-plugin` zip is not a single monolithic build — it is an **aggregate** of two
parts:

1. **Common plugin code built in this repo** — the same for every variant: `impl-cluster`, the
   `kettle-plugins-*` steps/entries, `shim-api-core`, `pentaho-hadoop-shims-common-base` (the shim
   framework/API), the HDFS VFS provider, etc.
2. **One Hadoop shim** — the distribution-specific piece, selected by the active profile
   (`-P<variant>`). `apachevanilla` is built here in the reactor; `cdp`, `hdi`, `emr` and `dataproc`
   are pre-built EE artifacts pulled from Artifactory with a pinned version.

In other words, each published zip is:

```
plugin-<variant>.zip = (common CE code, identical everywhere) + (shim for that variant)
```

The `maven-assembly-plugin` glues both parts together; the active profile decides *which shim* goes
in and sets the zip `classifier` (`-cdp`, `-emr`, …) and the `hadoop-configurations/<shim>` folder.

**Why this makes multi-shim testing important:**

- A fix in the **common CE code** is re-packaged into *every* variant zip, so it must be validated
  against *each* shim — the same change can behave differently on `emr` vs `dataproc` vs `cdp`
  because each shim wires up a different Hadoop distribution underneath.
- Changes to the **shim framework** (`shim-api-core` / `common-base`) are especially risky: the EE
  shims are compiled against a *pinned* version of that API, so an incompatible change can pass on
  `apachevanilla` (rebuilt in the reactor) yet break `emr`/`dataproc` (fixed EE artifacts) at
  runtime.
- The bug is often in the **interaction** between the common code and a specific shim, not in either
  alone — which only surfaces when the exact variant zip is exercised end to end.

Running the integration tests across the different variants is therefore the only reliable way to
catch shim-specific regressions before a release.

`assemblies/pentaho-big-data-plugin/pom.xml` defines one assembly profile per variant. They differ
only in which Hadoop shim they bundle:

| Profile | Bundled shim | Shim source |
| --- | --- | --- |
| `-Papachevanilla` | `pentaho-hadoop-shims-apachevanilla` (`${project.version}`) | **built in this repo** (reactor) |
| `-Pcdp` (default) | `pentaho-hadoop-shims-cdpdc71` | external EE artifact (Artifactory) |
| `-Phdi` | `pentaho-hadoop-shims-hdi40` | external EE artifact (Artifactory) |
| `-Pemr` | `pentaho-hadoop-shims-emr770` | external EE artifact (Artifactory) |
| `-Pdataproc` | `pentaho-hadoop-shims-dataproc23` | external EE artifact (Artifactory) |

To build and test another variant you always run **two** commands, and the profile (`-P<variant>`)
must be the same in both, plus the test variant (`-Dbigdata.it.plugin-variant=<variant>`) must match
that profile:

1. **Build & install** the project with the variant profile:

   ```
   mvn clean install -P<variant> -Dmaven.test.skip=true
   ```

2. **Run** the integration tests with the same profile and the matching variant:

   ```
   mvn -pl integration-test/pentaho-platform verify \
       -DrunIntegrationTests -P<variant> -Dbigdata.it.plugin-variant=<variant>
   ```

For example, for `hdi`:

```
mvn clean install -Phdi -Dmaven.test.skip=true

mvn -pl integration-test/pentaho-platform verify \
    -DrunIntegrationTests -Phdi -Dbigdata.it.plugin-variant=hdi
```

**Caveats:**

- **Only `apachevanilla` is built entirely in this repo.** Its shim comes from the reactor
  (`${project.version}`). The `cdp`, `hdi`, `emr` and `dataproc` shims are external EE artifacts
  (`org.pentaho.hadoop.shims:pentaho-hadoop-shims-*`) that must be resolvable from Artifactory.
- **The build profile and the test variant must match.** The harness unpacks the zip with
  `classifier=${bigdata.it.plugin-variant}` (default `apachevanilla`). If you build with a variant
  profile but leave the default, it will look for the `apachevanilla` zip and fail — always pass
  `-Dbigdata.it.plugin-variant=<variant>` together with `-P<variant>`.
- This *builds* the variant locally. To instead test an already-**published** variant without
  building it, use the download mode below.

### Nightly (test a published variant resolved from the Maven repository)

```
mvn -pl integration-test/pentaho-platform verify \
    -DrunIntegrationTests \
    -Dbigdata.it.source=download \
    -Dbigdata.it.plugin-variant=dataproc
```

`bigdata.it.plugin-variant` can be `apachevanilla`, `cdp`, `hdi`, `emr` or `dataproc`. In download
mode the plugin is **not** built locally; the `maven-dependency-plugin` unpack step resolves the
`pentaho-big-data-plugin` zip for the requested `classifier` (variant) and `version`
(`${project.version}`) directly from the configured Maven repository (Artifactory) and unpacks it
into the `pdi-client` container. No `-am clean` is needed here either; the plugin unpack dir is
wiped each run so a previous variant's shim does not leak into the container.

## Useful properties

| Property | Default | Meaning |
| --- | --- | --- |
| `bigdata.it.pdi-version` | `11.1` | PDI / pdi-client image tag |
| `bigdata.it.source` | `local` | `local` (unpack built zip) or `download` |
| `bigdata.it.plugin-variant` | `apachevanilla` | plugin classifier to install |
| `bigdata.it.named-cluster` | `it-cluster` | named cluster name in the generated metastore |
| `bigdata.it.parallel` | `false` | run tests in parallel |
| `test-network-name` | `bigdata-it-network` | Docker network name (`use-existing-docker-network` to reuse) |

## Notes / current status

- `BasicIT` (smoke) and `HdfsIT` (HDFS round trip) are the active tests.
- `HBaseIT` and `FormatsIT` are `@Disabled` pending validation of their KTR fixtures and the
  backend container images.
- The Hadoop container image and its `docker/hadoop` config/scripts, and the exact HDFS/HBase
  images, still need validation on an environment with Docker + registry access; adjust the
  `hadoop-image` / `hbase-image` properties and the container `wait`/entrypoint settings in
  `pentaho-platform/pom.xml` as needed.
