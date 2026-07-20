# NexusN3 Edge Core MCP User Value Proposal

**A user-centred operations and analysis layer over NexusN3 Edge Core (Nexus N3 Core)**  
Feasibility-aligned revision | July 2026

> **Proposal in one sentence**  
> The MCP layer should help an operator prepare, run, monitor, validate, and review a capture session safely, while converting available runtime state and session evidence into clear answers about readiness, session quality, missing samples or timing gaps, directly evidenced transport loss, failures, and the next action to take.

## Executive Summary

The MCP layer should be designed around the work a user is trying to complete, rather than around the internal services and data structures exposed by NexusN3 Edge Core. Its primary user story is to help an operator complete a trustworthy capture session from preparation through final review.

Diagnostics remain a first-class capability. Users should be able to ask whether a session was good, whether expected rates and durations were achieved, whether data was incomplete, and whether capture and finalization completed cleanly. The MCP should answer using explicit evidence and state the limits of that evidence. In particular, the MVP should report missing samples or timing gaps inferred from timestamps and expected rates, while describing packet loss only where the transport or sensor provides direct counters, sequence numbers, or equivalent evidence.

The recommended initial priorities are:

1. guided session preparation and operation
2. live session state and diagnostic guidance
3. post-session quality validation, visualisation, and analysis

The initial implementation can build strongly on existing readiness, lifecycle, startup, diagnostics, error, output, and finalization evidence. Composite workflows such as preflight and validated start are feasible, but they must be implemented by the MCP server as orchestration over existing commands and emitted events. Universal live packet-loss measurement, a generic per-algorithm completion ledger, and support-bundle packaging require additional runtime product work and should not be implied as existing MVP capabilities.

## 1. Purpose and Scope

This proposal defines the user-facing value and initial scope of an MCP layer that sits over a running NexusN3 Edge Core system. The edge runtime remains responsible for sensor communication, streaming, plugin execution, persistence, and session lifecycle management. MCP provides a safe, structured interface for questions, interpretation, and constrained operational workflows.

### Included

- User-facing readiness and session preflight assessment assembled from available runtime state.
- Guided discovery, connection, start, stop, and finalization using existing commands and subsequent events.
- Live interpretation of session lifecycle, sensor connection state, startup/warmup, gateway diagnostics, storage status, and available sensor counters.
- Explicit session-quality assessment based on missing samples or timing gaps, observed rates, partial capture, write failures, output presence, and finalization evidence.
- Transport packet-loss reporting where direct transport or protocol evidence exists.
- Recent-session summaries, comparison, operator reporting, and visualisation of supported raw data, computed events, diagnostics, and analysis results grounded in persisted artifacts.

### Excluded from the initial scope

- Editing `.env` files or low-level runtime configuration.
- Deployment, installation, operating-system administration, or topology changes.
- Arbitrary shell execution on an edge node.
- Replacing the existing runtime, admin UI, gateway transport, or file browser.
- Inventing a parallel source of truth outside the existing runtime and session artifacts.
- Claiming universal live packet-loss measurement where transports or plugins do not expose sufficient evidence.
- Claiming deterministic completion for every algorithm without a corresponding completion ledger or output evidence.
- Secret-safe support-bundle packaging until a defined collection and sanitization mechanism exists.

## 2. Core Product Position

> **Core position**  
> The NexusN3 MCP layer is an operator and analysis assistant. It helps users complete capture sessions successfully, understand what is happening, judge the quality and completeness of the resulting data, and identify what to do when something goes wrong. NexusN3 Edge Core remains the source of truth and owner of execution.

This positioning is stronger than presenting MCP as another status API or a natural-language wrapper around existing admin controls. The value comes from combining available runtime signals into a user-level answer, while preserving deterministic system behaviour and clearly stating where conclusions are direct, inferred, or unavailable.

## 3. Primary User Journey

1. **Define and prepare:** Capture the intended subjects, sensor types, body locations, requested algorithms, and expected output categories. Match detected devices to required sensor slots and present a unified readiness assessment across services, gateway, storage, capabilities, assignments, and requested algorithms.
2. **Start:** Orchestrate existing discovery, connection, and start commands, observe subsequent events, and explain whether the requested operation completed or what blocked it.
3. **Monitor:** Show the current lifecycle phase, sensor connection and activity, startup or warmup state, gateway and queue warnings, write errors, and any continuity indicators supported by the active sensors or transports.
4. **Stop and observe finalization:** Issue the existing stop operation, monitor subsequent lifecycle and persistence events, and report what evidence exists that writes, outputs, and archiving completed or were interrupted.
5. **Validate, visualise, and review:** Answer whether the session appears usable, what was missing or degraded, what outputs were produced, what evidence exists for analysis completion or interruption, and whether the run should be accepted, annotated, or repeated. Where supported, allow the user to graph raw or computed data and run approved analyses over selected session intervals.

## 4. Prioritized User Capabilities

### Priority 1: Guided Session Preparation and Operation

Help the operator define the intended capture, prepare the required subjects, sensors, assignments, and algorithms, and then start and safely stop the session without interpreting multiple internal commands and events.

**Typical user questions**
- Create a session with two subjects. Each subject should have a Movella DOT on the left ankle, a Movella DOT on the right ankle, and a Movesense sensor on the chest.
- What sensors are currently available, and can they satisfy this session design?
- What algorithms can be applied to this sensor configuration?
- Can you assign the detected sensors to the required subjects and body locations?
- Is everything assigned correctly and ready to start?
- If not, what exactly is missing, unavailable, or mismatched?
- After stopping, did the session finalize correctly?

The user defines the subject count, sensor types, body locations, and per-subject structure. The MCP converts this into a temporary session intent, proposes matches between detected devices and required sensor slots, validates applicable algorithms, and explains any missing or incompatible elements. Ambiguous device assignments should be confirmed by the operator.

**Core capabilities**

- Capture a temporary session intent defining the subjects, required sensor types and locations, sensor assignments, requested algorithms, and expected output categories where these can be determined.
- Propose matches between detected devices and required sensor slots, validate compatibility, and ask the operator to resolve or confirm ambiguous assignments.
- Present a single user-facing preflight result assembled from the session intent and available readiness, gateway, storage, plugin, sensor, assignment, and algorithm information.
- Implement `prepare_session` and validated start as composite MCP workflows over existing discover, connect, start, status, and event surfaces.
- Correlate requested operations with later success, error, lifecycle, and timeout events within the MCP layer.
- Return the final observed state and supporting evidence rather than only acknowledging that a command was sent.
- Highlight blocking issues and the corrective action required.

**Implementation note:** NexusN3 Edge Core does not currently expose a single native session-definition or preflight command, or a universal command-status object. The MCP server must orchestrate existing subject definitions, assignments, selected algorithms, runtime commands, and emitted events. The temporary session intent is an orchestration context only; NexusN3 Edge Core remains the source of truth for live runtime state.

**User outcome:** The operator can define the intended capture, confirm that the required subjects, sensors, assignments, and algorithms are ready, and complete the normal session lifecycle without navigating individual services, commands, or logs.

### Priority 2: Live Session State and Diagnostic Guidance

Interpret the current session using the evidence available during capture and make diagnostics directly useful while there is still time to intervene.

**Typical user questions**

- Are we officially streaming or still warming up?
- Are all sensors still connected and active?
- Are observed rates or recent counters showing a problem?
- Are there missing samples, timing gaps, or directly evidenced transport losses?
- Is there enough evidence to continue, or should I stop and inspect the session?

**Core capabilities available for an MVP**

- Summarize lifecycle phase, startup stability, sensor connection/disconnection state, recent activity, queue depth, first-notify latency, generic sensor counters, gateway diagnostics, and raw write failures where exposed.
- Identify clear operational problems such as failed startup, sustained disconnection, queue pressure, gateway errors, partial activity, or storage/write failure.
- Report timing or continuity concerns live only where current counters, timestamps, or transport-specific evidence make the conclusion reliable.
- Present concise evidence and a recommended operator action rather than exposing a raw event feed.

**Capability boundary:** A generic, accurate live packet-loss percentage is not available across all current transports and plugins. Universal live continuity assessment requires additional instrumentation, such as consistent sequence counters, expected-sample accounting, and transport-specific loss metrics. Until then, the MCP should use narrower wording and identify the evidence source.

**User outcome:** The operator understands the current state, sees actionable warnings supported by available evidence, and knows when a definitive quality decision must wait for post-session analysis.

### Priority 3: Post-Session Quality Validation, Visualisation, and Analysis

Provide an evidence-based answer about whether a completed session is complete, trustworthy, and suitable for its intended analysis, and allow the user to inspect the data and outputs that support that decision.

**Typical user questions**

- What was the quality of the session?
- Were any samples missing or were there significant timing gaps?
- Is transport packet loss directly evidenced for any stream?
- Did each stream run for the expected duration and approximate rate?
- Which requested outputs were produced, and what evidence exists for algorithm completion or interruption?
- Show the computed events produced during this session.
- Plot the sensor data around a detected gap or event.
- Compare the outputs from the two subjects or from two sessions.
- What supported analyses can be run over this session data?
- Should this session be accepted, annotated, or repeated?

**Core capabilities**

- Validate expected sensor and stream presence, approximate durations and rates, timestamp continuity, lifecycle completion, diagnostics, raw write failures, output presence, archive existence, and finalization metadata.
- Classify the result as `valid`, `valid_with_warnings`, or `invalid` using explicit, traceable checks.
- Separate direct evidence from inference and identify checks that could not be completed.
- Explain the result in user language and preserve the underlying evidence for review.
- Generate concise operator summaries and compare available quality indicators across sessions.
- Provide visualisations and supported analysis of raw data, computed events, and diagnostics, filtered by session, subject, sensor, location, metric, and time range.

**User outcome:** The user receives a defensible quality decision based on the evidence the platform actually records and can inspect the relevant raw data, computed events, diagnostics, and analysis results without manually locating session files.

## 5. Session Quality and Continuity Diagnostics

Session quality is not a secondary technical feature. It is one of the principal reasons to add an MCP layer: users need a direct answer to whether a run produced usable data and why. The MCP should combine deterministic checks with concise interpretation, without overstating what the current runtime can prove.

### 5.1 Terminology and evidence levels

The MCP should distinguish three different conclusions:

1. **Directly evidenced transport loss:** Sequence counters, explicit dropped-packet counters, or equivalent transport/plugin evidence show that packets were lost.
2. **Inferred missing samples or timing gaps:** Timestamp progression, expected rate, duration, or sample counts indicate missing intervals, but the exact transport cause is not proven.
3. **Continuity unknown:** The available runtime or persisted evidence is insufficient to quantify continuity reliably.

A packet-loss percentage should only be reported under the first category. The second category should use wording such as “missing sample intervals,” “timing gaps,” or “fewer samples than expected,” and should describe the inference method.

### 5.2 Evidence trusted by the MVP

The initial validation should rely on evidence classes already available or straightforward to surface:

- server readiness and current node role
- session lifecycle and startup gate events
- warmup and official-streaming transitions
- sensor connection and disconnection state
- available gateway diagnostics and generic per-sensor counters
- queue depth, first-notify latency, and recorded runtime errors
- raw write failures
- diagnostics summary and diagnostics events persisted with the session
- expected and observed sample information where present in session artifacts
- output-file presence
- archive existence and finalization metadata
- recent runtime events relevant to the session

Output presence and related events can provide evidence that an algorithm ran or produced a result. They should not be treated as a universal per-algorithm completion ledger unless the runtime adds explicit requested/started/completed/failed records for each algorithm.

### 5.3 Assessment coverage

Using the evidence available for a given session, the assessment should cover:

- lifecycle completeness
- expected sensor and stream coverage
- missing samples or timing gaps
- directly evidenced transport loss, where available
- expected versus observed rate and duration
- transport instability and disconnects
- startup stability and timestamp discontinuities
- queue pressure and gateway errors
- raw write failures and missing outputs
- evidence of algorithm output, interruption, or unknown completion state
- archive and finalization status
- the effect of each issue on the intended use

### Illustrative session-quality answer

**Overall result:** Valid with warnings

- **Continuity:** Post-session timestamp analysis found 127 missing sample intervals in the left-ankle IMU stream, approximately 0.2% of the expected samples. The sensor protocol did not expose a sequence counter, so this is reported as inferred missing samples rather than confirmed transport packet loss.
- **Capture and storage:** No raw write failures were recorded and the session archive is present.
- **Analysis outputs:** Expected IMU output files are present. No ECG output was produced because the ECG sensor disconnected before official streaming. The current evidence does not provide a separate completion ledger for every algorithm.
- **Recommendation:** The session appears usable for IMU analysis. Repeat it if ECG is required.

## 6. User-Level Workflows and Supporting Primitives

### Composite MCP workflows

These are user-facing operations implemented by the MCP server through orchestration, event observation, correlation, and timeout handling over existing NexusN3 commands and state:

- `run_session_preflight`
- `prepare_session`
- `start_validated_session`
- `assess_live_session`
- `stop_and_observe_finalization`
- `validate_session`
- `summarize_session`
- `compare_sessions`
- `generate_operator_report`
- `explore_session_data`

### Supporting primitives

- `get_system_status`
- `list_connected_sensors`
- `list_installed_capabilities`
- `get_latest_diagnostics`
- `get_recent_runtime_events`
- `request_gateway_status_snapshot`
- `get_output_destination_status`
- `list_session_outputs`
- `list_session_data`
- `query_session_data`
- `run_supported_session_analysis`

The higher-level workflows should answer common user questions in one operation. Supporting primitives remain useful for composability, evidence, troubleshooting, and advanced clients.

### Deferred product capabilities

The following are valuable but should be described as later product work rather than as simple MCP surfacing:

- universal live packet-loss and sequence-continuity accounting
- a generic per-algorithm requested/started/completed/failed ledger
- automatic secret-safe support-bundle collection and packaging
- cross-plugin signal-quality rules that require sensor-specific instrumentation

## 7. Visualisation and Session Data Analysis

The MCP should allow users to inspect and analyse persisted session data without manually locating and opening individual files.

Basic visualisation of known computed outputs and diagnostic timelines should be included in the MVP where schemas are already understood. More general raw-data visualisation and cross-format analysis should be introduced progressively as stream metadata, readers, and adapters are added.

**Typical user questions**

- Show the loading-intensity events for both subjects over the session.
- Plot the left- and right-ankle outputs for Subject 1.
- Show the raw accelerometer data around this event.
- Graph the ECG signal for a selected time range.
- Compare the computed outputs from two sessions.
- Run an available analysis over the raw or computed session data and explain the result.
- Are there gaps, discontinuities, or unusual values in this data?

**Core capabilities**

- Discover the raw data, computed events, diagnostics, and summaries available for a session.
- Provide a session data catalog describing available subjects, sensors, locations, streams, algorithms, metrics, time ranges, and supported analyses.
- Filter data by subject, sensor, body location, stream, algorithm, metric, and time range.
- Return structured, bounded, and where necessary downsampled data suitable for graphing in a Webview.
- Display time-series graphs, event markers, diagnostic timelines, and comparisons between subjects, sensors, or sessions.
- Run supported, predefined analyses over raw or computed session files.
- Explain the results and identify the source files, metrics, and time ranges used.
- Link computed events back to the relevant raw-data interval where timestamps allow this.

The MCP should distinguish between:

- retrieving and visualising existing outputs
- calculating simple derived summaries
- invoking an installed and approved analysis capability

It should not execute arbitrary user-supplied code over session files. Analysis should be restricted to supported operations and available algorithm capabilities.

**Implementation note:** Event timelines, diagnostics summaries, and known computed outputs can be surfaced using existing session artifacts. More general visualisation and analysis across raw-data formats will require stream metadata, file readers, downsampling rules, and analysis adapters for the supported sensor and algorithm-output schemas.

## 8. Feasibility and Implementation Split

| Capability | Can ship using current runtime evidence | Needs MCP orchestration | Needs new runtime instrumentation or product work |
|---|---|---|---|
| Readiness and live system summary | Yes | Light aggregation and interpretation | No |
| Guided discover/connect/start/stop | Yes | Yes: command/event correlation, workflow state, timeouts | No native unified command lifecycle today |
| User-facing session preflight | Largely | Yes: combine existing readiness and status evidence | Additional checks only if evidence is not currently exposed |
| Startup and warmup interpretation | Yes | Light interpretation | No |
| Live sensor connection and activity warnings | Yes | Aggregation and recommendations | Better universal activity/rate metrics would improve coverage |
| Universal live packet-loss percentage | No | Not sufficient by itself | Yes: consistent sequence counters or transport-specific loss metrics |
| Post-session missing-sample and timing-gap analysis | Often, where timestamps/rates are persisted | Yes: analysis and explanation | Plugin/runtime additions where required evidence is absent |
| Storage, raw-write, archive, and finalization checks | Yes | Aggregation and validation rules | No for current evidence classes |
| Per-algorithm completion validation | Partially through output presence and events | Yes: evidence interpretation | Yes for a deterministic generic completion ledger |
| Operator session report | Yes | Yes: assemble and explain evidence | No |
| Visualisation of known computed outputs and diagnostic timelines | Often, where output schemas are known | Yes: discovery, filtering, transformation, and graph-ready responses | Output adapters for unsupported schemas |
| Raw sensor time-series visualisation | Partially | Yes: file selection, range queries, downsampling, and graph-ready responses | Readers and stream metadata for each supported format |
| Supported analysis over session data | Partially | Yes: select and invoke approved analysis operations | Analysis adapters and capability metadata where not already available |
| Generic analysis across arbitrary files | No | Not sufficient by itself | Yes: standard schemas, readers, limits, and an approved analysis framework |
| Secret-safe support bundle | No complete mechanism today | Packaging workflow required | Yes: collection policy, sanitization, and bundle format |

This split should remain visible in planning so the MCP is not presented as a thin wrapper where orchestration or runtime instrumentation is actually required.

## 9. Recommended MCP Resources and Webview

### Resources

- `resource://system/readiness`
- `resource://system/capabilities`
- `resource://session/current`
- `resource://session/current/sensor-health`
- `resource://session/current/diagnostics`
- `resource://session/current/timeline`
- `resource://sessions/recent`
- `resource://session/<id>/validation`
- `resource://session/<id>/outputs`
- `resource://session/<id>/data-catalog`
- `resource://session/<id>/events`
- `resource://session/<id>/data`
- `resource://session/<id>/analysis`
- `resource://session/<id>/report`

Open Webview should render decision-oriented views rather than reproduce the existing admin dashboard. Initial views should include a preflight checklist, current-session overview, sensor-health matrix based on available evidence, summarized event timeline, active warnings, final validation report, and session comparison. Where supported by the available session schemas, it should also display computed-event timelines, raw and computed time-series graphs, diagnostic markers, and selected data intervals used by an analysis.

## 10. Trust, Safety, and Behaviour

- NexusN3 Edge Core and its persisted session artifacts remain the source of truth.
- Quality labels are derived from explicit checks and thresholds; the language model explains the result but does not invent it.
- Every diagnostic conclusion identifies whether it is direct, inferred, or unavailable.
- Composite actions report the MCP workflow state and the observed underlying runtime events. This must not be represented as a native unified command lifecycle in the core runtime.
- Workflow operations include bounded timeouts, correlation rules, and clear failure explanations.
- Start and stop requests should be idempotent where the underlying runtime supports it; otherwise the MCP must detect and explain conflicting state.
- Disruptive actions should require confirmation where appropriate.
- The MCP exposes only bounded runtime operations and never arbitrary command execution.
- Audit logging, support packaging, and sanitization should only be claimed when corresponding mechanisms are implemented.

## 11. Initial MVP

The first release should prove the user journey using capabilities that are feasible with current evidence plus MCP-layer orchestration:

1. `run_session_preflight`
2. `prepare_session`
3. `start_validated_session`
4. `assess_live_session`
5. `stop_and_observe_finalization`
6. `validate_session`
7. `summarize_session`
8. `explore_session_data`

The MVP should be able to answer:

- Can the intended session start based on the evidence currently available?
- Which preparation step or runtime condition is blocking progress?
- Are the required sensors connected, and what does the runtime indicate about current activity?
- Is the session officially streaming, warming up, stopped, or in an error state?
- Are there live warnings from startup, queue, gateway, sensor, or write diagnostics?
- Did post-session analysis identify missing samples or timing gaps?
- Is transport loss directly evidenced for any stream, or is continuity only inferred or unknown?
- Were expected raw and computed outputs produced?
- What evidence exists for algorithm completion or interruption?
- What raw streams, computed events, diagnostics, and metrics are available for this session?
- Can a selected sensor, computed output, event interval, or diagnostic interval be graphed?
- What supported analyses can be applied to this session data?
- Did storage and archive finalization appear to complete?
- Is the session usable, usable with warnings, or unsuitable for its intended purpose?

Basic visualisation of known computed outputs and diagnostic timelines is an MVP goal where schemas and readers already exist. Generic visualisation or analysis across arbitrary raw-data formats is not an MVP acceptance criterion. Universal live packet-loss measurement, deterministic per-algorithm completion, and automatic support-bundle generation are also excluded unless the required runtime instrumentation is added.

## 12. Success Criteria

- An operator can complete the normal session lifecycle without inspecting logs or manually correlating low-level events.
- The MCP clearly distinguishes direct transport-loss evidence from inferred missing samples or timing gaps.
- Live warnings identify the affected sensor or subsystem, the supporting evidence, the likely consequence, and the recommended action.
- A completed session receives a validation result with traceable evidence and explicit unknowns.
- A user can visualise known computed events and relevant diagnostic or sensor data without manually locating and interpreting session files.
- Composite workflows are clearly implemented as MCP orchestration over existing commands and events.
- The MCP does not create a competing runtime source of truth or bypass existing safety controls.
- The Webview helps the user make operational decisions rather than merely displaying more telemetry.

## 13. Recommendation

> **Recommended user story**  
> A user can define and prepare a NexusN3 session, verify readiness using the evidence the platform currently exposes, begin capture through a guided MCP workflow, understand the current session state and actionable diagnostics, stop safely, and receive an evidence-based decision on session completeness and quality. The user can then inspect supported raw data, computed events, diagnostics, and analysis results through graphs and approved analysis workflows. The result clearly distinguishes confirmed transport loss, inferred missing samples or timing gaps, and areas where the available evidence is insufficient.

Build the MCP around this journey. Diagnostics should remain explicit and central, but the proposal must stay aligned with what the runtime can currently prove. The first release should emphasize session-intent capture, readiness, guided operation, live state interpretation, post-session continuity analysis, visualisation of known outputs and diagnostics, storage and finalization evidence, and transparent limitations. Additional adapters and runtime instrumentation can then extend the product toward broader raw-data analysis, universal live packet-loss accounting, deterministic algorithm completion, and automated support packaging.