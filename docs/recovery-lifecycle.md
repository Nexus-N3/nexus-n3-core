# Session recovery and local validation — Core 0.1.16

Milestones 1–3 support Finish Session → Reset → Configure New Session within
the same Core process. Live gateway and sensor validation is still required.

## Operator behaviour

Finish Session drains and preserves captured data even when physical STOP is
unconfirmed. The archive remains partial where appropriate. Reset does not
finalise that archive again.

Reset verifies UART communication and requests the gateway's connection
inventory. If UART reopening is needed, Core preserves the configured modem
line state and does not issue `reset_session`, toggle DTR/RTS after opening,
flush buffered UART output or run the destructive startup handshake.

For an unconfirmed sensor, Core retains the original plugin host and its
measurement/subscription context. It first uses a surviving BLE connection.
If that identity has lost its connection, it attempts discovery and reconnection
before calling the existing plugin STOP interface. A failed STOP keeps its
measurement obligation and useful BLE link; disconnection alone never confirms
measurement STOP. A failed best-effort write also prevents STOP confirmation,
even if the plugin returns no result.

Reconnection restores retained notification and indication subscriptions so
plugins can receive their protocol's STOP responses. Recovery repairs stale
Core connection state before binding the retained plugin context. Gateway BLE
recovery does not accept an empty legacy STOP as confirmation without an
acknowledged adapter operation or verified earlier STOP completion. Those
checks are runtime infrastructure; sensor plugin hooks and contracts stay
unchanged.

The UI clears its configuration only after the expected Core participants
confirm configuration readiness. It reports gateway availability separately
from per-sensor STOP confirmation and acquisition eligibility. An unavailable
gateway does not prevent a responsive Core from accepting configuration.
Unreconciled physical addresses remain excluded from connection/acquisition.

Retry Reset after restoring UART communication or bringing a sensor into range.
An unavailable plugin context or a STOP that cannot be confirmed requires
operator intervention, potentially a sensor power cycle. Neither a timeout nor
an empty BLE inventory is treated as proof that measurement has stopped.

After physically stopping or power cycling the listed sensors and checking
their measurement indicators, the operator can explicitly confirm that they
have stopped. That action authorises a controlled BLE reset, verifies gateway
reset completion and retires unusable sensor host context before releasing the
addresses. Results identify this evidence as operator confirmation. Ordinary
Reset never makes this claim or automatically performs that escalation.

## Internal boundaries

- Reset uses the existing `disconnect_all` command with `recover_session: true`.
  Existing correlation IDs, participant announcements and cleanup-completion
  events continue to carry node-specific outcomes. Ordinary Disconnect retains
  its separate command behaviour.
- Recovery is serialised with SensorManager commands and repeated Reset requests
  coalesce. Conflicting acquisition/configuration requests are rejected until
  completion. Old cleanup deadlines are replaced only for a fresh operation.
  Configuration replacement has a 55-second operation fence within its existing
  60-second Core wait; expired work cannot commit a later configuration.
- Operation IDs and deadlines fence old RPC and adapter calls. The host runtime
  barrier rejects recovery while old lifecycle work is still running; it does
  not replace the sensor object. Timed-out cleanup no longer kills a useful host.
- Recovery has an 80-second overall deadline, up to 45 seconds per sensor, and
  up to 15 seconds for each measurement STOP. The UI watchdog remains 90 seconds.
  Normal acquisition retains Milestone 1's 15-second sensor STOP, 90-second
  overall physical STOP and 95-second Core wait.
- UART readers have a transport generation fence. Lock acquisition and serial
  writes are bounded, complete writes are checked, and UART draining is avoided.
- Gateway reset and acquisition use a shared admission guard. A destructive BLE
  reset remains explicit and cannot run while another subject is acquiring or
  starting. Controlled escalation is available through an explicitly supplied
  `allow_ble_reset: true` recovery payload; ordinary Reset does not request it.
  Reset uncertainty is cleared by a matching late completion or a confirmed
  controlled reset, rather than inferred from connection absence.
- Scoped recovery quiesces only targeted routes. Other subjects retain raw and
  computed recording. STOP notifications identify only the affected subjects.
- Obligations survive subject/configuration replacement in the same Core process,
  together with their original sensor and plugin context. Unknown inventory
  identities are retained as unresolved rather than given a fresh empty host.
- Configuration failures restore Core's prior subject graph and SensorManager's
  prior sensor configuration. Successfully recovered retired hosts are released.

No individual sensor plugin, plugin contract, catalog or firmware changes are
required. Distributed command routing is retained; this is node-local recovery,
not a distributed recovery redesign.

## Hardware smoke test

1. Complete a healthy session, Reset, configure and acquire again.
2. With two subjects, stop one and confirm the other's CSV and compute continue.
3. Interrupt UART while acquiring, Finish, and verify the partial archive.
4. Restore UART without rebooting the gateway, Reset, and confirm surviving
   sensors receive STOP before disconnecting. Configure and acquire again.
5. Repeat with a sensor out of range, then return it to range and retry Reset.
6. Leave UART unavailable and verify bounded recovery, configuration readiness
   and explicit resource unavailability. Repeat Reset without restarting Core.

Check physical sensor indicators, received samples and actual BLE inventory.
In particular, verify the Makerdiary USB/CDC modem-line behaviour on reopening.
Automated success does not prove hardware recovery. Commands already submitted
inside the gateway cannot be recalled by a host cancellation token; recovery
must respect gateway responses and retain uncertainty when confirmation fails.
