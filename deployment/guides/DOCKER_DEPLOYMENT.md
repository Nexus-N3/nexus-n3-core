# Docker Deployment

This guide covers two containerized standalone deployment shapes:

- runtime deployment using built `.rsnxplugin` bundles
- dev deployment using the repo checkout and mounted `nexus-n3-plugin-catalog/`

Use the runtime path for production-style installs. Use the dev path when
working from the source tree and iterating on plugins locally.

## Deployment Modes

### Runtime Mode

The production-oriented Docker artifacts are:

- `deployment/docker/Dockerfile.runtime`
- `deployment/docker/docker-compose.runtime.yml`

This shape does not require plugin tooling or plugin source trees inside the
runtime container.

The container entrypoint supports a mounted bundle directory through:

- `NEXUS_N3_PLUGIN_BUNDLE_DIR`

At startup, the container installs any bundle version that is not already
present in the plugin root.

## Runtime Host Directories

Create these directories on the target host:

```bash
sudo mkdir -p /srv/nexus-n3/config
sudo mkdir -p /srv/nexus-n3/plugin-bundles
sudo mkdir -p /srv/nexus-n3/outputs
```

Expected contents:

- `/srv/nexus-n3/config/runtime.env`
  - the runtime environment file
  - sudo cp runtime.env /srv/nexus-n3/config
- `/srv/nexus-n3/plugin-bundles/`
  - built `.rsnxplugin` bundles only
- `/srv/nexus-n3/outputs/`
  - runtime output directory mounted into the container

## Runtime Prerequisites

The target host should have:

- Docker Engine
- Docker Compose plugin
- access to the built `nexus-n3-core` repo checkout used for image build
- built plugin bundles ready for copy into `/srv/nexus-n3/plugin-bundles`

## Runtime Env

Create `/srv/nexus-n3/config/runtime.env`.

You can start from:

- `config/runtime.env`

Then adjust values for the target system.

Important values:

- `NEXUS_N3_PLUGIN_ROOT=/opt/nexus-n3-plugins`
- `BLE_BACKEND`
- `GATEWAY_SERIAL_PORT` if using the BLE gateway
- Azure bridge variables if enabled

`NEXUS_N3_PLUGIN_CATALOG_ROOT` is not used in this deployment shape.

## Runtime Bundle Preparation

Copy only built bundles into:

```text
/srv/nexus-n3/plugin-bundles
```

Recommended source layout on the build machine:

```text
nexus-n3-plugin-catalog/plugin-builds/
  sensors/
    rpi/
  algorithms/
    rpi/
```

For Raspberry Pi runtime deployment, copy the required `*-rpi.rsnxplugin`
artifacts from those target directories into `/srv/nexus-n3/plugin-bundles/`.

Do not copy:

- `nexus-n3-plugin-catalog/`
- plugin source repositories
- `nexus-n3-plugin-tooling`

## Runtime Launch

From `nexus-n3-core`:

```bash
cd deployment/docker
docker compose -f docker-compose.runtime.yml up --build
```

## Runtime Plugin Installation

At container startup:

1. the entrypoint reads `NEXUS_N3_PLUGIN_BUNDLE_DIR`
2. it scans for `*.rsnxplugin`
3. it checks whether that exact plugin version already exists in
   `/opt/nexus-n3-plugins/installed/<plugin_id>/<version>/`
4. if not installed, it runs `python -m nexus_n3.plugins install ...`

This keeps plugin deployment aligned with the production artifact model.

Because the container entrypoint installs bundles locally inside the container
plugin root, there is no host-side plugin permission normalization step here.
The runtime env file still needs to be readable by the container process
through the mounted config path.

## Dev Mode

The source-oriented Docker artifacts are:

- `deployment/docker/Dockerfile`
- `deployment/docker/docker-compose.dev.yml`

This shape is intended for local development from the checked-out repo. It
mounts the plugin source workspace directly:

- `../../../nexus-n3-plugin-catalog:/workspace/nexus-n3-plugin-catalog`

It also mounts the repo runtime env file directly:

- `../../config/runtime.env:/app/nexus-n3-core/config/runtime.env:ro`

It binds the host plugin root directly into the container:

- `/opt/nexus-n3-plugins:/opt/nexus-n3-plugins`

Use this mode when:

- you are developing or testing plugins from `nexus-n3-plugin-catalog/`
- you do not want to stage built bundles into `/srv/nexus-n3/plugin-bundles`
- the host environment is a poor fit for the runtime bind mounts

The dev compose file starts the server with:

- `--admin`

Runtime settings still come from `config/runtime.env`.

### Dev Prerequisites

The repo should contain:

- `config/runtime.env`
- `nexus-n3-plugin-catalog/`
- `/opt/nexus-n3-plugins` on the host

If plugin bootstrapping is enabled in `runtime.env`, the selected plugins are
built from the mounted `nexus-n3-plugin-catalog/` tree and installed into the host-backed
plugin root at `/opt/nexus-n3-plugins`.

Newly installed plugins become available to the runtime for future discovery
and future session setup without rebuilding the image. Sessions are still
expected to be configured against a stable plugin set: the required sensors and
algorithms should already be installed when a session is created.

Clients that define sessions should query the live supported plugin inventory
before building or submitting a new session configuration. Subject/session
initialization validates the requested sensors and algorithms against the
currently installed plugin catalog; it does not discover plugin options on its
own.

### Dev Launch

From `nexus-n3-core`:

```bash
cd deployment/docker
docker compose -f docker-compose.dev.yml up --build
```

## Notes

- This Docker path is currently documented for standalone deployment.
- Distributed plugin-aware scheduling is not yet implemented in the runtime.
- If you need to update one plugin quickly, replace the bundle in
  `/srv/nexus-n3/plugin-bundles` and restart the container.
- Production-style Docker deployment should use built target-specific bundles
  such as `*-rpi.rsnxplugin`, not plugin source trees.
