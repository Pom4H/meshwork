# meshwork

A tiny capability-based compute mesh. The first real worker target is an iPad Pro M1 running WebGPU in Safari.

## iPad WebGPU worker

The broker serves the worker UI itself. Start it on a machine reachable from the iPad:

```bash
bun install
bun run broker
```

WebGPU requires a secure context when the iPad connects over the network. Run the broker with a certificate trusted by the iPad:

```bash
bun src/cli.ts broker --tls-cert ./cert.pem --tls-key ./key.pem
```

Then open `https://<broker-host>:8787/` on the iPad and tap **Start worker**. Keep the page visible and the iPad powered; the worker requests a screen wake lock while connected.

Check that the node is registered:

```bash
bun src/cli.ts workers --broker https://<broker-host>:8787
```

Submit a GPU frame:

```bash
bun src/cli.ts run render.webgpu.frame.v1 \
  --broker https://<broker-host>:8787 \
  --input '{"width":1280,"height":720,"seed":1}'
```

The result contains the PNG artifact URL plus render timing measured on the iPad.

## Reliability model

Every assignment has a unique `attemptId` and a renewable lease. Heartbeats extend the lease while the worker is alive. If a worker disappears or the lease expires, the broker requeues the task. Late results from stale attempts are ignored, so a disconnected device cannot overwrite a newer execution.

Artifacts are uploaded before `task.result` and stored under `.meshwork/artifacts`.

## CLI

```
meshwork broker [--port 8787] [--tls-cert CERT --tls-key KEY]
meshwork worker [--broker URL] [--name NAME]
meshwork run <capability> [--input JSON] [--broker URL]
meshwork workers [--broker URL]
```
