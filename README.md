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

## Render streams

Long-running video is modeled as causes rather than as unrelated frame jobs:

```
Stream
  Epoch
    Segment
      Task
        Attempt
```

A stream owns stable transport/render properties such as resolution, FPS, codec and segment size. An epoch is the current world/render cause: `sceneHash`, `renderHash`, snapshot and starting frame. A segment is a contiguous timeline range handed to a worker. The existing task lease remains the retry primitive underneath it.

Create a 4K60 stream:

```bash
curl -X POST http://localhost:8787/streams \
  -H 'content-type: application/json' \
  -d '{
    "capability":"render.video.segment.v1",
    "width":3840,
    "height":2160,
    "fps":60,
    "codec":"hevc",
    "segmentFrames":30,
    "warmupFrames":8,
    "sceneHash":"scene-sha256",
    "renderHash":"renderer-sha256",
    "snapshotHash":"snapshot-sha256"
  }'
```

Queue segments with one request:

```bash
curl -X POST http://localhost:8787/streams/<stream-id>/segments \
  -H 'content-type: application/json' \
  -d '{"count":8}'
```

Free compatible workers receive different contiguous segments. Fast workers naturally consume more segments because the scheduler assigns the next deadline as soon as a worker becomes free. Workers that just completed the same `sceneHash:renderHash` affinity are preferred, so hot GPU/assets caches stay useful.

For per-segment simulation snapshots, pass `snapshots` with one content hash per segment. A worker gets `warmupStartFrame` and `warmupFrames`; warmup frames are rendered to reconstruct temporal state such as TAA/history but are not part of the published segment.

When input, camera, scene or renderer cause changes, create a new epoch:

```bash
curl -X POST http://localhost:8787/streams/<stream-id>/epochs \
  -H 'content-type: application/json' \
  -d '{
    "startFrame":240,
    "sceneHash":"new-scene-sha256",
    "renderHash":"renderer-sha256",
    "snapshotHash":"snapshot-240-sha256"
  }'
```

Creating an epoch cancels every pending/running older segment. Late results from the old epoch cannot complete because their task is already cancelled. This keeps the published timeline causally consistent.

`GET /streams/<stream-id>/segments` returns segment tasks ordered by epoch/frame for a muxer/reorder buffer. Workers should upload encoded fragments (for example fMP4) before returning `task.result`; `video/mp4` artifacts are stored as `.m4s`.

## Reliability model

Every assignment has a unique `attemptId` and a renewable lease. Heartbeats extend the lease while the worker is alive. If a worker disappears or the lease expires, the broker requeues the task. Late results from stale attempts are ignored, so a disconnected device cannot overwrite a newer execution.

For streams there is a second causality barrier: an `Epoch` invalidates older pending/running segments when the source state changes.

Artifacts are uploaded before `task.result` and stored under `.meshwork/artifacts`.

## CLI

```
meshwork broker [--port 8787] [--tls-cert CERT --tls-key KEY]
meshwork worker [--broker URL] [--name NAME]
meshwork run <capability> [--input JSON] [--broker URL]
meshwork workers [--broker URL]
```
