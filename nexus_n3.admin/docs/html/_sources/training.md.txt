# Training

## Goal

New operators should be able to:

- start the runtime
- verify the admin UI
- confirm plugin inventory
- understand where session outputs are written
- perform a plugin-bundle install when required

## Basic Exercises

### 1. Start The Runtime

```bash
nexus-n3-core
```

This assumes the runtime is configured through `/etc/nexus-n3/runtime.env` or
an override file selected with `NEXUS_N3_ENV_FILE`.

### 2. Confirm Admin Access

Start with admin enabled if needed:

```bash
nexus-n3-core --role standalone --admin --admin-host 0.0.0.0 --admin-port 9000
```

### 3. Confirm Plugin Discovery

At startup, verify the plugin summary line appears and matches the expected
installed sensor and algorithm plugins.

### 4. Run Health Check

```bash
bash scripts/health_check.sh
```

### 5. Install A Plugin Bundle

```bash
python -m nexus_n3.plugins install /path/to/plugin.rsnxplugin --plugin-root /opt/nexus-n3-plugins
```
