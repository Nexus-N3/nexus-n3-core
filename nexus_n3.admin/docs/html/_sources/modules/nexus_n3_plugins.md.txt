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
- `nexus_n3.plugins/runtime/runtime.py`
- `nexus_n3.plugins/runtime/sensor_runtime.py`
- `nexus_n3.plugins/runtime/sensor_host.py`
- `nexus_n3.plugins/runtime/algorithm_host.py`
- `nexus_n3.plugins/__main__.py`
