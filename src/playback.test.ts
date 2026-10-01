import { expect, test } from "bun:test";
import { playlist, splitMp4, playableSegments } from "./playback";
import type { StreamState, TaskState } from "./protocol";

const stream: StreamState = {
  spec: {
    id: "stream",
    capability: "render.video.segment.v1",
    width: 3840,
    height: 2160,
    fps: 60,
    codec: "avc",
    segmentFrames: 120,
    warmupFrames: 24,
  },
  currentEpoch: {
    streamId: "stream",
    id: 0,
    startFrame: 0,
    sceneHash: "scene",
    renderHash: "renderer",
    createdAt: 0,
  },
  nextFrame: 360,
  status: "active",
  createdAt: 0,
  updatedAt: 0,
};
const segment = (
  frame: number,
  status: TaskState["status"] = "completed",
  epoch = 0,
): TaskState => ({
  task: {
    id: "task-" + frame,
    capability: stream.spec.capability,
    cause: { streamId: "stream", epoch, startFrame: frame },
    input: { startFrame: frame, frameCount: 120 },
  },
  status,
  attempts: 1,
  createdAt: 0,
  updatedAt: 0,
  output: {
    artifact: {
      url: `https://broker.example/artifacts/task-${frame}/attempt.m4s`,
    },
  },
});

test("out-of-order completions never publish beyond a timeline gap", () => {
  const states = [
    segment(240),
    segment(0),
    segment(120, "running"),
    segment(0, "completed", 1),
  ];
  expect(
    playableSegments(stream, states).map((s) => s.task.cause!.startFrame),
  ).toEqual([0]);
  expect(playlist(stream, states)).not.toContain("task-240");
  states[2]!.status = "completed";
  const text = playlist(stream, states);
  expect(text.indexOf("task-0")).toBeLessThan(text.indexOf("task-120"));
  expect(text.indexOf("task-120")).toBeLessThan(text.indexOf("task-240"));
  expect(text).toContain(
    '#EXT-X-MAP:URI="/artifacts/task-120/attempt.m4s?part=init"',
  );
  expect(text).not.toContain("https://broker.example");
});

test("rolling live playlists retain correct sequence numbers after pruning", () => {
  const live = {
    ...stream,
    spec: { ...stream.spec, live: true },
    playbackStartFrame: 120,
    mediaSequence: 1,
  };
  const states = Array.from({ length: 24 }, (_, n) => segment((n + 1) * 120));
  const text = playlist(live, states);
  expect(text).toContain("#EXT-X-MEDIA-SEQUENCE:13");
  expect(text.match(/#EXTINF:/g)?.length).toBe(12);
  expect(text).not.toContain("#EXT-X-PLAYLIST-TYPE:EVENT");
  expect(text).toContain("task-1560");
});

test("fragment splitting rejects truncated or non-fragmented uploads", () => {
  const box = (name: string, data = new Uint8Array()) => {
    const bytes = new Uint8Array(8 + data.length);
    new DataView(bytes.buffer).setUint32(0, bytes.length);
    bytes.set(new TextEncoder().encode(name), 4);
    bytes.set(data, 8);
    return bytes;
  };
  const data = new Uint8Array([
    ...box("ftyp"),
    ...box("moov"),
    ...box("moof"),
    ...box("mdat", new Uint8Array([1, 2, 3])),
  ]);
  expect(splitMp4(data).init.length).toBe(16);
  expect(splitMp4(data).media.length).toBe(19);
  expect(() => splitMp4(data.slice(0, -1))).toThrow();
  expect(() => splitMp4(box("moov"))).toThrow();
});
