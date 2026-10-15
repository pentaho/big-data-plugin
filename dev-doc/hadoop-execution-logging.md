# Hadoop Execution Logging

This document explains why the Big Data Plugin installs temporary Log4j
appenders for Hadoop-capable executions, the concurrency rules those appenders
must preserve, and how to investigate a future logging failure.

The bridge exists so a Kettle execution can receive diagnostic events emitted
by an embedded Hadoop client. A common example is the YARN application ID. The
event belongs in the log channel for the Kettle job or transformation that
submitted it, not in a process-wide log or another concurrent Carte run.

## Terms and scope

PMR (Pentaho MapReduce) is the PDI feature that packages Kettle map, combine,
and reduce transformations as a Hadoop MapReduce job. YARN is the Hadoop
service that schedules the submitted work and assigns an application ID. PMR
and YARN are related, but they are not interchangeable names.

This design covers embedded Hadoop-client logging for these Big Data Plugin
entry points:

| Integration | Logging boundary |
| --- | --- |
| Standard MapReduce | `JobEntryHadoopJobExecutor` |
| PMR/YARN | `JobEntryHadoopTransJobExecutor` |
| Pig | `JobEntryPigScriptExecutor`, including its asynchronous worker |
| Oozie | `OozieJobExecutorJobEntry` asynchronous runnable |
| Sqoop | `AbstractSqoopJobEntry` |

Spark is intentionally outside this mechanism. It starts an external
`spark-submit` process and forwards that process's stdout and stderr directly
to its Kettle log channel. HDFS copy does not start an independent Hadoop
process.

## Historical context

PDI-20391 reported that Hadoop process messages, including YARN application
IDs, appeared in the owning Kettle log before an upgrade but no longer did in
9.3.0.9.

Three historical changes matter, but they do not have the same status:

| Change | Confirmed effect | Relevance |
| --- | --- | --- |
| `BACKLOG-35556` | Added the earlier Sqoop log-channel filtering pattern. | Behavioral precedent for execution-specific routing. |
| `BACKLOG-35776` | Added `log4j-api` and `log4j-core` to the PMR runtime archive so secured HDP PMR jobs could run. | Correlates with the original PMR regression. Keep these dependencies; removing them reintroduces the secured-PMR failure. |
| `BACKLOG-47361` | Removed static Hadoop-related `INFO` logger entries from Kettle's default Log4j configuration. | A later, independent event-admission failure when runtime wiring is absent and root logging remains `ERROR`. |

The repository establishes the PMR archive change, the dedicated Hadoop
configuration classloader, and the observed regression. It does **not** prove
that the PMR archive and Kettle application use different Log4j
`LoggerContext` instances in the affected assembled runtime. That remains the
leading explanation for the original routing failure, not a confirmed fact.

The shim classloading design is documented in
[shim-bridging-classloading.md](shim-bridging-classloading.md). Its separate
classloader paths are relevant because Log4j configuration is bound to a
`LoggerContext`, not to a logger name across every classloader.

## Current design

`HadoopExecutionLogging` is the Big Data Plugin lifecycle owner. Entry points
open it immediately before invoking a Hadoop client and close it in the same
execution path, including asynchronous runnables. The class lives at
[`HadoopExecutionLogging.java`](../kettle-plugins/common/ui/src/main/java/org/pentaho/big/data/kettle/plugins/logging/HadoopExecutionLogging.java).

On start, it:

1. Saves the caller's existing Log4j `ThreadContext.logChannelId` value.
2. Sets that value to the owning Kettle `LogChannelInterface` ID.
3. Creates one uniquely named `KettleLogChannelAppender` per requested logger
   category.
4. Adds a filter that accepts only events tagged with that same channel ID.
5. Registers each appender through Platform's `LogUtil.addAppenderWithLevel`
   at `INFO` level.

On close, it removes the registrations in reverse order and restores the prior
thread-context value. Closing is idempotent, including the partial-startup
failure path.

The Platform helper owns shared `LoggerConfig` state. Its key is the identity
of the `LoggerContext` plus the logger name. While any registration is active,
the configuration is non-additive and permits the most verbose requested
level. When the final registration closes, it restores the prior level and
additivity, or removes the dynamic configuration it created.

This separation is deliberate:

- **Admission:** the temporary logger configuration lets Hadoop `INFO` events
  exist even when the root logger is `ERROR`.
- **Delivery:** the appender filter delivers an admitted event only to its
  owning Kettle log channel.

An appender filter cannot rescue an event rejected by its logger level.
Conversely, allowing `INFO` without a channel filter would mix concurrent
executions. Both parts are required.

```text
Kettle execution A starts Hadoop work
  -> ThreadContext.logChannelId = A
  -> filtered appender A is registered for the Hadoop category

Kettle execution B starts Hadoop work
  -> ThreadContext.logChannelId = B
  -> filtered appender B is registered for the Hadoop category

Hadoop event tagged A
  -> appender A accepts it and writes to Kettle log A
  -> appender B rejects it

Final active scope closes
  -> Platform restores the original logger configuration
```

## Concurrency invariants

Do not weaken these rules. They prevent a fixed missing-message bug from
becoming a mixed-log bug.

1. Do not change the root logger level to `INFO`. The bridge must be scoped to
   Hadoop work and must not add global third-party logging noise.
2. Do not use an unfiltered global appender. Each destination must filter on
   the execution's `logChannelId`.
3. Do not make the temporary logger additive. Hadoop events must not propagate
   to parent or root appenders while the bridge is active.
4. Do not restore logger configuration until the final active registration for
   that `(LoggerContext, logger name)` pair closes.
5. Start and close the lifecycle in the same execution thread. For asynchronous
   work, create the scope inside the worker runnable so its thread context
   receives the owning channel ID.
6. Do not assume that events emitted by arbitrary child threads inherit the
   `ThreadContext` tag. An untagged event is correctly rejected by every
   execution appender; the emitting path must propagate the context explicitly
   if it needs execution-specific routing.

## Sqoop is different

Sqoop is the only Big Data Plugin integration that also redirects JVM-global
`System.err`. It invokes Sqoop in-process, and some Sqoop diagnostics bypass
Log4j by writing directly to `System.err`. `LoggingProxy` converts those string
writes into `org.apache.sqoop` `INFO` events, which the normal filtered bridge
then routes to the owning Kettle log.

`System.err` is one mutable JVM-wide stream, not an execution-local resource.
`AbstractSqoopJobEntry` therefore reference-counts its redirect: a completed
Sqoop execution must not restore the original stream while another Sqoop run is
still active. For the same reason the single proxy cannot be bound to one
`Logger`: concurrent executions may use different shim classloaders, and each
one then owns a `Logger` in its own `LoggerContext`. The proxy resolves its
target on every write from a `ThreadLocal` that the execution sets while it
attaches and clears while it detaches, so a write always reaches the logger of
the execution that owns the calling thread. A write from a thread that owns no
execution falls back to the original `System.err`. Do not copy this pattern to
another integration unless that tool also runs in-process and demonstrably
bypasses Log4j. An external child process should have its own stdout and stderr
streams read directly, as Spark does.

[sqoop-stderr-routing.md](sqoop-stderr-routing.md) walks through why a proxy
bound to a single `Logger` breaks once shim classloaders get their own
`LoggerContext`, and which invariants keep the thread-local resolution correct.

## Failure diagnosis

Start with the following checks, in order. They distinguish the main failure
modes without turning the root logger into a diagnostic firehose.

1. **Confirm the boundary.** Verify that the relevant entry point opens
   `HadoopExecutionLogging` around the direct Hadoop-client call. Check the
   asynchronous worker path as well as the blocking path.
2. **Confirm logger admission.** Inspect the effective `LoggerConfig` for the
   emitting category. It must accept the event's level while the scope is
   active. A root `ERROR` configuration drops Hadoop `INFO` events before any
   appender filter runs.
3. **Confirm event tagging.** Inspect the event's context data for
   `logChannelId`. It must equal the owning Kettle log channel. This is the
   likely issue for logs emitted from a separate thread.
4. **Confirm context identity.** Record the identity of
   `LogManager.getContext(false)`, the emitting logger's class source, and the
   thread context classloader at both the bridge and the event source. If they
   resolve to different Log4j contexts, a registration in one cannot observe
   events emitted in the other.
5. **Confirm lifecycle timing.** Check that the scope remains open until the
   client finishes emitting the relevant status. A premature close detaches the
   appender and restores logger configuration.
6. **Check for cross-run leakage.** Run two overlapping executions and verify
   that each application ID appears once in its owner log and never in the
   other log.

If check 4 identifies separate contexts, do not remove the PMR Log4j archive
as a shortcut. The archive fixes secured PMR execution. Extend the bridge only
after identifying the context that actually receives the Hadoop event, while
preserving the same channel filtering and final-close restoration rules.

## Tests and remaining evidence

| Test | What it proves |
| --- | --- |
| Platform `LogUtilTest` | Scoped `INFO` admission, parent/root non-propagation, and final-close restoration. |
| `HadoopExecutionLoggingTest` | Concurrent synthetic YARN application IDs reach only their owner channels; duplicate categories are ignored; cleanup detaches appenders. |
| `AbstractSqoopJobEntryTest` | The `System.err` redirect remains installed until overlapping Sqoop executions finish. |
| `ThreadContextPropagatingExecutorServiceTest` | A pool task sees the `ThreadContext` of its submitter, the worker thread's own context is restored afterwards, and overlapping tasks keep the context captured at submit time. |
| Focused job-entry suites | Existing MapReduce, PMR, Pig, Oozie, and Sqoop behavior remains valid after lifecycle wiring. |

The fast tests do not prove the assembled PMR runtime or a real YARN
submission. The unresolved acceptance scenario is a gated integration test
against a supported Hadoop cluster:

1. Start two PMR/YARN jobs concurrently through an assembled distribution and
   Carte.
2. Collect the two Kettle execution logs after completion.
3. Assert that each log contains only its own real YARN application ID.
4. Capture context identity and classloader evidence during the same run.

A Pentaho Server action or schedule should run the same scenario as release
acceptance because it exercises the embedded-engine path in Tomcat.

## Manual reproduction with a local Docker cluster

This procedure reproduces PDI-20391 on a developer machine (macOS, Docker
Desktop) with the Hadoop Job Executor sample jobs. It compares a build without
`HadoopExecutionLogging` against one with it. It was exercised with 11.0.0.3-312
(before the fix) and 11.1.0.0-436 (with the fix).

### What to compare

| Build | Shim | Expected Spoon log pane for the entry |
| --- | --- | --- |
| 11.0.0.3-312 (no `HadoopExecutionLogging`) | `cdpdc71` | Only Kettle's own lines: progress (`Setup Complete: … Mapper Completion: …`) and task events (`[SUCCEEDED] -- Task: attempt_…`). |
| 11.1.0.0-436 (fixed) | `apachevanilla` | The same lines plus the `org.apache.hadoop` client lines that the unfixed build prints only on the console, for example `Connecting to ResourceManager at …`, `Submitted application application_…` and `The url to track the job: …`. |

On the unfixed build the Hadoop client lines are not lost. They go to the
console (the terminal that launched `spoon.sh`) through Log4j's console
appender, in the console pattern (`17:39:10,551 INFO [RMProxy] …`) instead of
the Kettle pattern. That is the symptom: the lines exist but do not reach the
owning Kettle log channel.

Check that a build has the fix with
`unzip -l lib/pentaho-big-data-kettle-plugins-common-ui-*.jar | grep HadoopExecutionLogging`
in the big-data plugin directory.

### Builds and shims

Use one PDI folder per version, because all installs of one user share
`~/.kettle` and `~/.pentaho/metastore`.

- Extract the `pdi-ee-client` zip, then replace
  `data-integration/plugins/pentaho-big-data-plugin` with the contents of the
  matching big-data plugin zip.
- Set `active.hadoop.configuration` in the plugin's `plugin.properties`
  (`apachevanilla` for 11.1, `cdpdc71` for 11.0).
- Do not use the 11.0 `apache` or `emr` plugins as the baseline. The 11.0
  `apache` shim declares no MapReduce service in `shim_services.yaml`, so
  `JobEntryHadoopJobExecutor` ends with a null `MapReduceService`. The `cdp`
  plugin of the same build does declare it.
- For `cdpdc71`, copy the cluster's `core-site.xml`, `hdfs-site.xml`,
  `yarn-site.xml`, `mapred-site.xml` and `capacity-scheduler.xml` into
  `hadoop-configurations/cdpdc71/`.

### Cluster

Run Apache Hadoop 3.4.1 (`apache/hadoop:3.4.1`) as five containers:
`namenode`, `datanode`, `resourcemanager`, `nodemanager` and `historyserver`.
All of them mount the same `config/` directory at `/opt/hadoop/etc/hadoop`.
Settings that are easy to miss:

| Setting | Why |
| --- | --- |
| Publish `9000`, `9866`, `8032`, `8088`, `10020` and `40000-40010`. | HDFS RPC, DataNode data transfer, the ResourceManager, the job history and the MapReduce AppMaster port range. |
| `/etc/hosts` on the host: `127.0.0.1 namenode datanode resourcemanager nodemanager historyserver`. | The cluster hands out host names, not container IPs. |
| `dfs.client.use.datanode.hostname` and `dfs.datanode.use.datanode.hostname` set to `true`. | PDI must reach the DataNode through the host name. |
| `HADOOP_MAPRED_HOME=/opt/hadoop` on the `nodemanager`, and it listed in `yarn.nodemanager.env-whitelist`. | Without it the AppMaster fails with `Could not find or load main class ...MRAppMaster`. |
| `yarn.app.mapreduce.am.job.client.port-range=40000-40010` and `mapreduce.jobhistory.address=historyserver:10020` in the client-side `mapred-site.xml` (or as user-defined properties on the job entry). | PDI polls the AppMaster while the job runs and falls back to the history server afterwards. |
| For the Cloudera `cdpdc71` client only: in the cluster `core-site.xml`, set `rpc.engine.org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB` to `org.apache.hadoop.ipc.ProtobufRpcEngine2` with `<final>true</final>`. | The 3.1.x client writes the legacy `ProtobufRpcEngine` into `job.xml`. Hadoop 3.4.1 uses shaded protobuf, so the AppMaster dies with `NoClassDefFoundError: com/google/protobuf/ServiceException`, or with `ClassCastException` if `protobuf-java-2.5.0.jar` is added. The `apachevanilla` shim does not need it. |

Start Spoon, and Carte if you test on it, from a terminal with
`HADOOP_USER_NAME=hadoop`, otherwise HDFS rejects the local macOS user name.
Each process reads the variable when it starts, so a Carte started without it
runs as the macOS user even if Spoon, which sends it the job, has it.

Upload the sample input once and make it writable:

```text
docker cp <pdi>/data-integration/plugins/pentaho-big-data-plugin/samples/jobs/hadoop/excite-small.log namenode:/tmp/
docker exec namenode hdfs dfs -mkdir -p /wordcount/input
docker exec namenode hdfs dfs -put -f /tmp/excite-small.log /wordcount/input/
docker exec namenode hdfs dfs -chmod -R 777 /wordcount
```

### Named cluster

Create it in the Spoon UI: HDFS `namenode:9000`, user `hadoop`, job tracker
`resourcemanager:8032`, security None, then run **Test**. The shim is
`apachevanilla` on 11.1 and Cloudera Data Platform (CDP) 7.1 on 11.0.

Keep only one named cluster for this host in
`~/.pentaho/metastore/pentaho/NamedCluster/` at a time. The *Delete folders*
entry in the samples names only `hdfs://namenode:9000`, so a second named
cluster on the same host makes it fail. If you create a cluster by copying its
XML file, change the file name, the `<id>`, the `name` field and the trailing
`<name>` element; otherwise the copy is hidden behind the original. Restart
Spoon after editing the metastore by hand.

### Sample jobs

The samples are the `.kjb` files `Hadoop Job Executor adv.kjb` (Advanced mode)
and `Hadoop Job Executor simple.kjb` (Simple mode) in
`plugins/pentaho-big-data-plugin/samples/jobs/hadoop/`. The `.ktr` files in
that folder are the Pentaho MapReduce mapper and reducer and are not used here.
Change these values in the job entry, then save:

| Setting | Sample default | Change to |
| --- | --- | --- |
| Cluster | none | the named cluster |
| HDFS host and port | `hadoop-server:8020` | `namenode:9000` |
| Job tracker | `hadoop-server:8021` | `resourcemanager:8032` |
| Jar path | `./samples/jobs/hadoop/pentaho-mapreduce-sample.jar` (not resolved) | the absolute path of that jar in the install |
| Advanced mode, user-defined properties | placeholders | add the AppMaster port range and `mapreduce.jobhistory.address` shown above |
| Simple mode, arguments | `--hdfsHost=hadoop-server:8020 --jobTrackerHost=hadoop-server:8021` | `--hdfsHost=namenode:9000` |
| *Clean Output* entry | `hdfs://hadoop-server:8020/wordcount/output` | `hdfs://namenode:9000/wordcount/output` |

Spoon rewrites the whole file when saving, so a plain diff against the original
is noisy. Only these values matter.

### Confirm the run really succeeded

The Hadoop Job Executor entry can finish green while the YARN application is
`FAILED`. Confirm with the ResourceManager UI (`http://localhost:8088`) or:

```text
docker exec namenode hdfs dfs -ls /wordcount/output      # expects _SUCCESS and part-r-00000
docker exec nodemanager sh -c 'ls /var/log/hadoop/userlogs/<application_id>/*'
```

The `syslog` of the `..._000001` container is the AppMaster log. The job fails
if `/wordcount/output` exists before the run; the *Clean Output* entry removes
it.

### Known gaps

- **Simple mode (fixed, single-run verified).** `FutureMapReduceJobSimpleImpl`
  runs the driver `main()` on a pool thread created by
  `Executors.newCachedThreadPool()` in the initializer. That thread had no
  `logChannelId`, so its events were rejected by the filtered appender
  (invariant 6). Observed on 11.1.0.0-436 before the fix: in one Spoon session,
  against one cluster and one named cluster, `Hadoop Job Executor adv.kjb` showed
  the client lines in the entry log and `Hadoop Job Executor simple.kjb` showed
  only Kettle's own `Waiting for execution of … to finish...` line once per
  second, although the YARN application (`WordCount`) finished `SUCCEEDED` and
  wrote `/wordcount/output`. The thread explanation came from inspecting the
  shipped classes, not from a runtime trace.
  The pool is now wrapped by `ThreadContextPropagatingExecutorService`
  (services-bootstrap), which runs each task with the `ThreadContext` map of the
  submitting thread. It has to be applied in two places:
  `BigDataCEServiceInitializerImpl` (CE distribution) and
  `BigDataEEServicesInitializerImpl` (separate repository, packaged in
  `services-bootstrap-ee-*.jar`). The EE class extends the CE one but overrides
  `initializeMapReduceServices` and creates its own `newCachedThreadPool()`
  instances, one for the plain MapReduce factory and one for the impersonation
  factory, so both are wrapped there. The EE jar shades the CE
  `services-bootstrap` classes, so build order is: install `services-bootstrap`
  here, then build `services-bootstrap-ee`. Do not replace the EE jar with the
  CE jar to test: the EE initializer disappears and the named cluster's HDFS
  tests fail.
  After the rebuild, one run of `Hadoop Job Executor simple.kjb` on 11.1.0.0-436
  (EE client, `apachevanilla`) showed the `WordCount - Simple` entry log with
  `Connecting to ResourceManager…`, `Submitting tokens for job: job_…`,
  `The url to track the job: …/application_…/`, `Running job: job_…`, the
  `map/reduce` progress lines, `completed successfully` and the counters. The
  once-per-second `Waiting for execution of … to finish...` lines remain
  interleaved. Not covered: the Knox MapReduce factory (no pool is created for
  it in the initializer) and `simple_blocking=N` (the scope closes before the
  driver finishes).
- **Message formatting (fixed in the appender, root cause still in Kettle).**
  Before the fix, messages with `{}` placeholders appeared in the Kettle log as
  raw `ParameterizedMessage[messagePattern=…]` text, for example
  `Connecting to ResourceManager at {}`, `Executing with tokens: {}`,
  `Unable to find '{}'.` and `Submitted application {}`. `Log4jKettleLayout`
  (kettle-core, used by the appender in `HadoopExecutionLogging`) reads the
  message with `LogEvent.getMessage()`, handles only Kettle's `LogMessage` and
  otherwise falls back to `Object.toString()`; it never calls
  `getFormattedMessage()`, and `ParameterizedMessage.toString()` does not
  include the arguments. The effect was that the `Submitted application` line
  did not show the application ID in the entry log (it was still visible in
  `The url to track the job: …/application_…/` and in
  `Submitting tokens for job: job_…`). The appender now uses
  `FormattedMessageKettleLayout`, a private subclass that passes the layout a
  copy of the event whose message is the already formatted text, except for
  Kettle `LogMessage` and `SimpleMessage`, which are left untouched.
  `HadoopExecutionLoggingTest#rendersParameterizedMessagesWithTheirArguments`
  fails with the previous layout and passes now. The same `Log4jKettleLayout`
  limitation still affects any other caller in Kettle; this change only fixes
  the Hadoop appender. Verified on 11.1.0.0-436 with `Hadoop Job Executor
  simple.kjb`: the entry log now shows
  `Connecting to ResourceManager at resourcemanager/127.0.0.1:8032`,
  `Executing with tokens: []`, `Unable to find 'resource-types.xml'.` and
  `Submitted application application_…`.
- **Isolation (checked in Spoon and on Carte, Simple mode).** Two copies of
  `Hadoop Job Executor simple.kjb` (`simple_a`, `simple_b`, with `--output` and
  the Clean Output folder set to `/wordcount/output_a` and `/wordcount/output_b`)
  were started about six seconds apart in the same Spoon session on
  11.1.0.0-436 (EE client, `apachevanilla`). The `WordCount - Simple` entry log
  of A contained only `job_1791897433339_0013` / `application_1791897433339_0013`
  and the log of B only `…_0014`; no line of one run appeared in the other.
  YARN reported both applications `FINISHED` / `SUCCEEDED` and both output
  folders contain `_SUCCESS` and `part-00000`. Limitation of this run: both
  drivers were alive and tracking at the same time (B submitted while A was
  running), but B's job only started executing after A had finished, so the
  map/reduce progress lines of the two runs did not interleave in time.
  The same two jobs were then sent to a Carte started from the same install with
  `HADOOP_USER_NAME=hadoop` (A and B ten seconds apart, YARN applications
  `…_0015` and `…_0016`). Each log contained only its own `job_…` /
  `application_…` IDs, both applications ended `SUCCEEDED` and both output
  folders have `_SUCCESS`. The limitation repeated: A's application ran from
  19:27:59 to 19:29:07, B's was submitted at 19:28:08 but its first progress
  line (`running in uber mode`, `map 0% reduce 0%`) appeared at 19:30:42, after A
  had finished. Not yet checked: more than two runs, and runs whose map/reduce
  phases really overlap (needs a cluster that schedules both applications at
  once), and Advanced mode concurrency.

## Guidance for maintainers and agents

When adding a Hadoop-capable execution path, use this checklist:

1. Identify every logger category that emits user-relevant status for the
   operation.
2. Open `HadoopExecutionLogging` immediately around the direct client call.
3. For asynchronous work, open and close it inside the worker that emits or
   invokes the client logging.
4. Ensure failure and cancellation paths close the scope.
5. Add a focused test that emits a real Log4j event while the entry executes,
   then verifies delivery during the scope and no delivery after it closes.
6. Preserve the two-execution isolation test whenever changing filtering,
   logger categories, or lifecycle ownership.

Do not reintroduce static global Hadoop `INFO` logger declarations as a
replacement for this lifecycle. Static admission does not establish ownership,
does not isolate concurrent executions, and changes logging behavior for the
entire host process.
