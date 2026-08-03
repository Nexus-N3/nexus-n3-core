# Nexus N3 Core Plugins

`nexus_n3.plugins` contains the install, catalog, and runtime support for
`nexus-n3-core`.

The package is now organized into subpackages:

- `install/`
  Bundle validation, installation, activation, and catalog persistence.
- `runtime/`
  Discovery, isolated algorithm host runtime, JSON-RPC transport, and runtime serialization.
- `common/`
  Small shared helpers used by both install and runtime code.

## What This Module Does

- resolves the configured plugin root
- validates `.rsnxplugin` ZIP bundles
- enforces Phase 1 safe ZIP rules
- creates versioned installed plugin directories
- creates one `.venv` per installed plugin version
- installs plugin wheel artifacts into that `.venv`
- runs import and health checks from the plugin runtime
- records install metadata and failure metadata
- updates the installed plugin catalog
- persists the active version in the plugin catalog
- maintains POSIX `current` compatibility symlinks
- provides a small developer CLI for bundle install and plugin-root inspection
- provides dev-plugin build+install helpers for local source trees

## What Phase 1 Does Not Do

- no `SensorProxy`
- no live plugin host process supervision
- no JSON-RPC lifecycle for live sensor sessions
- no adapter proxy or BLE forwarding over plugin IPC
- no plugin-to-plugin runtime sample routing
- no callback compatibility layer for isolated sensor hosts yet

Those belong to later phases described in
`plans/RUNTIME_PLUGIN_DEPLOYMENT_PLAN.md`.

## Plugin Root

The plugin root is configurable.

Resolution order:

1. explicit argument such as `--plugin-root`
2. `NEXUS_N3_PLUGIN_ROOT`
3. existing Nexus OS config if wired in by the caller
4. production default `/opt/nexus-n3-plugins`

Development example:

```text
/path/to/nexus-n3-plugins
```

## Phase 1 Layout

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
      current -> <version>/  # POSIX compatibility link; omitted on Windows
  failed/           # optional
  cache/            # optional
  catalog/
    plugins.json
    <plugin_id>.json
    install_failures.json
```

## Main Packages

- `install/config.py`
  Resolves plugin root configuration.
- `install/layout.py`
  Defines the plugin filesystem layout.
- `install/bundle.py`
  Validates and extracts `.rsnxplugin` bundles safely.
- `install/installer.py`
  Installs bundles into the configured plugin root.
- `install/catalog.py`
  Persists plugin catalog and failure records.
- `runtime/runtime.py`
  Resolves installed external algorithm plugins and launches isolated hosts.
- `runtime/transport.py`
  Implements the Phase 3 stdio JSON-RPC transport.
- `runtime/algorithm_host.py`
  Runs an external algorithm plugin inside its installed plugin `.venv`.
- `runtime/discovery.py`
  Reads installed plugin metadata for support and capability listing.
- `common/jsonio.py`
  Shared JSON helpers.
- `__main__.py`
  Minimal CLI entry point.

## Developer CLI

Show the resolved plugin root:

```bash
python -m nexus_n3.plugins show-root --plugin-root /tmp/nexus-n3-plugins
```

Install a bundle into a chosen root:

```bash
python -m nexus_n3.plugins install /path/to/plugin.rsnxplugin --plugin-root /tmp/nexus-n3-plugins
```

Activate an already-installed version and safely remove its inactive
superseded versions:

```bash
python -m nexus_n3.plugins activate \
  --plugin-id standard-loading-intensity \
  --version 0.1.2 \
  --plugin-root /opt/nexus-n3-plugins

python -m nexus_n3.plugins prune-inactive \
  --plugin-id standard-loading-intensity \
  --keep-version 0.1.2 \
  --plugin-root /opt/nexus-n3-plugins
```

Pruning refuses to run unless `--keep-version` is already the active version.

Install one or more dev plugins directly from the source tree workspace:

```bash
python -m nexus_n3.plugins install-dev \
  --nexus-n3-plugin-catalog-root /path/to/nexus-n3-plugin-catalog \
  --plugin movella-dot \
  --plugin standard-loading-intensity \
  --plugin-root /opt/nexus-n3-plugins
```

Install the dev-plugin list defined in `config/runtime.env` without starting
the server:

```bash
python -m nexus_n3.plugins install-dev-list --plugin-root /opt/nexus-n3-plugins
```

Show the current runtime-env dev-plugin list:

```bash
python -m nexus_n3.plugins show-dev-list
```

Important: when `prepare_dev_plugins()` is used without an explicit build root,
the intermediate `.rsnxplugin` bundles are built in a temporary directory and
removed after install. If you want to retain the built bundles, either:

- build them explicitly with `nexusn3-plugin build --output-dir ...`, or
- pass `--plugin-build-root` to `install-dev` or `install-dev-list`

Bundle creation is no longer part of `nexus-n3-core`.
Use `nexus-n3-plugin-tooling` to scaffold and build `.rsnxplugin` artifacts.

## Reference Plugins

Phase 1 uses these as the concrete migration examples:

- sensor: `nexus-n3-plugin-catalog/sensors/nexus-n3-sensor-movella-dot`
- algorithm: `nexus-n3-plugin-catalog/algorithms/nexus-n3-algorithm-standard-loading-intensity`

## Tests

Targeted plugin runtime coverage lives in:

```text
nexus_n3_tests/plugins/test_phase1_installer.py
nexus_n3_tests/plugins/test_phase2_catalog_discovery.py
nexus_n3_tests/plugins/test_phase3_algorithm_runtime.py
```

That test slice covers:

- plugin root precedence
- safe ZIP rejection rules
- install activation behavior
- failed install behavior
- catalog-based discovery
- host-backed external algorithm runtime
