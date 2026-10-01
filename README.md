# meshwork

A capability-based compute mesh. Multiple iPads render different frame ranges of one game-engine scene and encode them locally. The broker publishes the ordered fragments as one 3840 × 2160 HLS stream, without rendering or transcoding the video on the host.

## Distributed scene rendering

```text
game-engine build + scene + textures → immutable scene bundle
                                          ↓
                              Meshwork broker / scheduler
                                ↙                  ↘
                  iPad A: frames 0–119       iPad B: frames 120–239
                  WebGPU → WebCodecs        WebGPU → WebCodecs
                                ↘                  ↙
                              ordered fMP4 / HLS playlist
                                          ↓
                              Safari native HLS / Hls.js
```

Install dependencies, build the game-engine checkout containing your scene, then register it. That engine build must include `WebGPURenderer.setTimelineFrame()` and `resetTemporalHistory()` (added for absolute TAA jitter and segment warmup).

```bash
bun install
bun src/cli.ts scene --engine ../game-engine --entry tools/arcade/scene.mjs --name snowy-arcade-fidelity
bun src/cli.ts broker
```

Generate the scene's local texture assets before registration. The bundle freezes the built `dist/` runtime and the scene entry's directory, including JS modules and textures. Relative module imports are preserved; the adapter remaps bundled `/tools/` and `/assets/` material texture URLs. Both scene and renderer content hashes must match. Missing or modified files are rejected.

The iPads need a secure context for WebGPU. Use a trusted HTTPS reverse proxy (including WebSocket forwarding), or the broker's TLS options:

```bash
bun src/cli.ts broker --tls-cert ./cert.pem --tls-key ./key.pem
```

For a temporary development link, `cloudflared tunnel --url http://localhost:8787` can forward a local broker over HTTPS. Set a random `MESHWORK_ACCESS_TOKEN` environment variable **before starting the broker** when exposing it beyond a trusted network. Set the same variable for CLI commands. The stream command prints worker and player links with an access key; opening either link grants an HttpOnly session cookie and removes the key from the address bar. API clients use `Authorization: Bearer <key>`. The same protection applies to the worker WebSocket and scene assets. `--host 127.0.0.1` binds the broker to loopback for a local tunnel.

Start one stream:

```bash
bun src/cli.ts stream --scene snowy-arcade-fidelity --broker https://<broker-host>
```

Defaults: 3840 × 2160, 60 FPS timeline, AVC/H.264, 120 frames per segment, 24 warmup frames, live queue. Override with `--width`, `--height`, `--fps`, `--codec avc|hevc`, `--segment-frames` (maximum 240), and `--warmup-frames`.

Open the printed **workerUrl on each iPad**, enter a distinct name, and tap **Start worker**. Keep Safari in the foreground and the devices powered. The page requests a screen wake lock and probes 720p60 AVC/HEVC WebCodecs support, then verifies the actual task dimensions; only workers advertising the selected codec receive video jobs. Open **playerUrl on a viewing device**. The broker keeps generating ranges while the stream is active, so nodes may join after it starts.

```bash
bun src/cli.ts workers --broker https://<broker-host>
bun src/cli.ts stop-stream <stream-id> --broker https://<broker-host>
```

Each worker uses deterministic fixed-step simulation, absolute frame indices for TAA jitter, and hidden warmup frames. It retains scene/GPU caches between assignments and replays simulation when seeking. Every segment begins with a keyframe, carries its own decoder configuration, and uses absolute presentation timestamps. The broker exposes only the contiguous completed prefix: a later segment never skips a missing range. Faster nodes take more ranges; codec compatibility and scene affinity guide scheduling.

The live queue holds two ranges per compatible worker (minimum two, maximum 32), including completed ranges waiting behind gaps. It retains 24 contiguous segments on disk and advertises the latest 12 in the playlist. Lost nodes or expired leases requeue the same range under a new attempt ID; old uploads/results are rejected. A rendering failure retries on another compatible node, up to three attempts. A terminal failure stops the stream and cancels remaining work.

## Performance and current limits

4K60 is a **target timeline**, not a measured throughput guarantee. Real-time playback requires the mesh to render and upload at least 60 usable frames per second in aggregate; warmup, encoding and scene loading also consume time. Two-second segments add buffering latency. More nodes help when rendering is the bottleneck, but do not eliminate a missing early segment or insufficient network bandwidth. `prefer-hardware` is an encoder preference, not proof that the browser chose a hardware encoder.

Scenes without an audio entry produce video only. Register `--audio tools/arcade/carriage-audio.mjs` to add the scene's deterministic stereo soundtrack. The broker needs ffmpeg and muxes AAC with copied video packets, keeping the iPad encoder video-only. Audio uses one global 1024-sample grid, disjoint packet ownership and preroll to avoid repeated encoder priming gaps. The sound button explicitly unmutes the player. Engine snapshot restore and arbitrary interactive input replication are not implemented. Register only trusted scene modules, since workers and the audio broker execute them. Immutable bundles and artifact files persist. Streams are held in memory; local embedding can pass trusted `initialStreams` to restore configurations in a new fenced epoch.

## Live chat and scene direction

The player includes shared chat and five reactions. SSE pushes updates; polling recovers when a connection fails. History and reaction totals persist in `.meshwork/chat`. Messages render as text, with bounded length and request throttling.

```bash
codex login
bun src/cli.ts broker --codex-director
```

Set `MESHWORK_CODEX_BIN` if Codex is not on PATH. The installed CLI reuses its saved authentication, runs ephemerally with user config and execution rules disabled, and returns a schema-constrained Russian reply and scene patch. Shell, file, plugin and connector tools are disabled. [Codex non-interactive documentation](https://learn.chatgpt.com/docs/non-interactive-mode) describes CLI authentication and structured outputs. Requests run serially, with an eight-message queue and a 90-second timeout. If Codex is unavailable, chat says so and video continues; there is no simulated agent fallback.

The director can change fog, exposure, sky/moon colors, snowfall, wind, lantern brightness, camera speed and soundtrack volume. It can add/remove up to 48 simple opaque primitives, for example benches and barrels. More complex models, texture generation or renderer changes require an offline build; the agent reports this limit. Scene patches contain only validated finite data, never executable code or URLs.

Changes take effect at the first unrendered boundary after already leased/completed work, and update unleased future tasks. Rendering controls interpolate over one second using absolute frames. Camera speed integrates continuously instead of jumping to a new rail phase. The existing HLS epoch, timestamps and GPU device are preserved. The chat labels the source frame as queued and marks it visible when playback reaches that frame, using HLS program time. Renderer/network throughput and normal HLS buffering still determine the delay; this is not zero-latency video. Refresh older worker pages once: live edits require `scene.live-controls.v1`.

`GET /streams/<id>/chat` returns room state; `GET /streams/<id>/events` subscribes to SSE. `POST /streams/<id>/chat` accepts `{name,text,clientId}` and `POST /streams/<id>/reactions` accepts `{emoji,clientId}`. Existing optional broker authentication applies to these routes too.

## Stream API

`POST /streams` creates a stream with `capability`, `width`, `height`, `fps`, `codec`, `segmentFrames`, `warmupFrames`, `sceneHash`, `renderHash`, and optional `startFrame` / `live`. `render.video.segment.v1` requires registered hashes and rejects snapshot restoration. Set `live: true` for automatic queuing; omit it for finite batches and `POST /streams/<id>/segments` with `{ "count": 8 }`. Manual ranges cannot overlap.

`POST /streams/<id>/epochs` accepts new registered `sceneHash`, `renderHash` and optional `startFrame`. It invalidates older pending/running work. `GET /streams/<id>/segments?epoch=<n>` lists results. `GET /streams/<id>/playlist.m3u8` returns fMP4 HLS, and `POST /streams/<id>/stop` freezes playback at the completed prefix. `GET /scenes` lists registered bundles.

The upstream shard API is also available: `POST /shards` leases long-running simulation work, `POST /shards/<id>/epochs` changes its cause, and `POST /shards/<id>/stop` revokes it. Workers publish monotonic ticks under a fenced lease. A worker with an active shard cannot receive a simultaneous ordinary task. Shards require a worker implementing their capability; the browser video worker still renders stream segments.

Browser workers probe codecs at 720p60 and validate the exact dimensions, frame rate, bitrate and realtime encoder mode for each assignment. If the hardware preference is unsupported, they try `no-preference`. The scene runtime owns its GPU device; the demo frame device is created only for demo frame tasks. On failure, a separate **Last error** panel preserves the error, stage and browser version while retries continue. Reload the worker page to receive updated browser code. A codec list alone does not confirm that scene initialization or encoding succeeded.

Generic task capabilities remain available (`echo`, `system.info` with the Bun worker, and `render.webgpu.frame.v1` with the browser worker):

```bash
bun src/cli.ts worker --broker http://localhost:8787 --name cpu-node
bun src/cli.ts run echo --input '{"message":"hello"}'
```

## Verification

```bash
bun test
bun run check
bun tools/qa-video.ts snowy-arcade-fidelity 3840
```

The integration harness requires native Chrome, `ffprobe`, and `ffmpeg` (override `CHROME_PATH`, `FFPROBE_PATH`, `FFMPEG_PATH` if needed). It creates two isolated browser workers with a real GPU, renders two actual scene segments, checks decoded pixel variation, frame counts, 4K dimensions and absolute timestamps, then plays their combined HLS stream. Evidence, decoded previews and a player screenshot are saved under `artifacts/qa-video/`. This validates the desktop path; physical iPad compatibility and sustained throughput require testing on the actual devices.
## Browser worker recovery

Browser workers answer broker-driven heartbeat requests as well as their own
timer. Timeline replay yields in short slices and reports progress, including
during warmup. Transient disconnects automatically retry with a 1–30 second
backoff; returning to the tab or restoring network connectivity retries sooner.
The last close code and reason stay visible after recovery. Invalid protocol or
authorization responses stop automatic retries. A suspended device still cannot
render until the browser resumes it.
