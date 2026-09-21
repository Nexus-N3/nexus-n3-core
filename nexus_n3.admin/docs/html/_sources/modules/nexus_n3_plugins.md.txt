# nexus_n3.plugins

## Overview

`nexus_n3.plugins` owns plugin installation, cataloging, discovery, and the
runtime host support used by Nexus N3 Core.

It is the bridge between:

- built `.rsnxplugin` artifacts
- the installed plugin root on disk
- runtime capability discovery
- isolated algorithm and sensor host execution

## Responsibilities

- resolve the plugin root
- validate `.rsnxplugin` ZIP bundles
- install bundles into versioned plugin directories
- create one `.venv` per installed plugin version
- persist plugin catalog state
- discover installed sensor and algorithm support
- launch isolated runtime hosts for supported plugin types
- expose local developer bootstrap helpers for `nexus-n3-plugin-catalog`

## Current Runtime Model

Production/operator-facing plugin artifacts are `.rsnxplugin` bundles.

Installation is handled with:

```bash
python -m nexus_n3.plugins install /path/to/plugin.rsnxplugin
```

For local development, the runtime can also build and install selected
`nexus-n3-plugin-catalog` entries:

```bash
python -m nexus_n3.plugins install-dev --nexus-n3-plugin-catalog-root /path/to/nexus-n3-plugin-catalog --plugin movella-dot
python -m nexus_n3.plugins install-dev-list
```

When building directly from the development workspaces, explicitly provide the
local SDK so the bundle contains the SDK revision used by the plugin source:

```bash
nexus-n3-plugin prepare \
  --plugin-root /path/to/nexus-n3-plugin-catalog/sensors/my-sensor \
  --sdk-root /path/to/nexus-n3-plugin-tooling/packages/sdk

nexus-n3-plugin build \
  --plugin-root /path/to/nexus-n3-plugin-catalog/sensors/my-sensor \
  --output-dir /path/to/nexus-n3-plugin-catalog/plugin-builds/sensors \
  --sdk-root /path/to/nexus-n3-plugin-tooling/packages/sdk \
  --target local
```

`prepare` installs the local SDK editably into the plugin's development
environment. `build` creates and includes a local SDK wheel for the isolated
installed runtime. Installed plugin versions are immutable, so changed bundle
contents require a new plugin version rather than overwriting an existing one.

## Sensor Host Wi-Fi Bridge

Installed Wi-Fi sensor plugins run in the same isolated host model as other
sensor plugins. The Core-side driver bridge exposes plugin methods for connected
discovery, candidate classification and identification, provisioning, vendor
connection, and disconnection over JSON-RPC.

The SDK also supplies optional `reset_session_diagnostics()` and
`get_diagnostics_snapshot()` hooks. The sensor host forwards them without making
diagnostics mandatory for existing plugins. Returned snapshots must be
JSON-serializable and must not contain credentials, native handles, or secret
vendor command payloads.

## Layout

```text
<plugin_root>/
  incoming/
  installed/
    <plugin_id>/
      <version>/
        bundle/
        runtime/
          .venv/
        manifest.json
        install.json
      current -> <version>/
  cache/
  failed/
  catalog/
    plugins.json
    <plugin_id>.json
    install_failures.json
```

## Runtime Notes

- installed sensors and algorithms are discovered from the plugin catalog
- sensor plugins are resolved into runtime classes before sensor-manager setup
- algorithm plugins are launched in isolated runtime hosts
- startup logging now reports the plugin root plus installed sensor/algorithm
  inventory visible to the runtime

## Key Files

- `nexus_n3.plugins/install/config.py`
- `nexus_n3.plugins/install/layout.py`
- `nexus_n3.plugins/install/installer.py`
- `nexus_n3.plugins/install/catalog.py`
- `nexus_n3.plugins/runtime/discovery.py`
- `nexus_n3.plugins/runtime/algorithm_runtime.py`
- `nexus_n3.plugins/runtime/sensor_runtime.py`
- `nexus_n3.plugins/runtime/sensor_host.py`
- `nexus_n3.plugins/runtime/algorithm_host.py`
- `nexus_n3.plugins/__main__.py`
