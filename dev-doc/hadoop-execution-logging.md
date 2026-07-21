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
still active. Do not copy this pattern to another integration unless that tool
also runs in-process and demonstrably bypasses Log4j. An external child process
should have its own stdout and stderr streams read directly, as Spark does.

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
