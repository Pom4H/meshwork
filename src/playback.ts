import type { StreamState, TaskState } from "./protocol";

/** Only expose the uninterrupted prefix, even when workers finish out of order. */
export function playableSegments(stream: StreamState, tasks: TaskState[]) {
  const ordered = tasks
    .filter((s) => s.task.cause?.epoch === stream.currentEpoch.id)
    .sort(
      (a, b) =>
        (a.task.cause?.startFrame ?? 0) - (b.task.cause?.startFrame ?? 0),
    );
  const result: TaskState[] = [];
  let frame = stream.playbackStartFrame ?? stream.currentEpoch.startFrame;
  for (const state of ordered) {
    const input = state.task.input as {
      startFrame: number;
      frameCount: number;
    };
    if (input.startFrame < frame) continue;
    if (
      input.startFrame !== frame ||
      state.status !== "completed" ||
      !state.output ||
      typeof state.output !== "object" ||
      !("artifact" in state.output)
    )
      break;
    result.push(state);
    frame += input.frameCount;
  }
  return result;
}

export function playlist(stream: StreamState, tasks: TaskState[]) {
  const all = playableSegments(stream, tasks);
  const segments = stream.spec.live ? all.slice(-12) : all;
  const target = Math.ceil(
    Math.max(
      stream.spec.segmentFrames / stream.spec.fps,
      ...segments.map(
        (s) =>
          (s.task.input as { frameCount: number }).frameCount / stream.spec.fps,
      ),
    ),
  );
  const lines = [
    "#EXTM3U",
    "#EXT-X-VERSION:7",
    "#EXT-X-INDEPENDENT-SEGMENTS",
    `#EXT-X-TARGETDURATION:${target}`,
    `#EXT-X-MEDIA-SEQUENCE:${(stream.mediaSequence ?? 0) + all.length - segments.length}`,
  ];
  if (!stream.spec.live) lines.push("#EXT-X-PLAYLIST-TYPE:EVENT");
  for (const state of segments) {
    const artifact = (state.output as { artifact: { url: string } }).artifact;
    // Each independently encoded segment carries its own decoder configuration.
    // Relative URLs survive HTTPS reverse proxies and do not expose localhost.
    const path = new URL(artifact.url, "http://localhost").pathname;
    lines.push(
      `#EXT-X-PROGRAM-DATE-TIME:${new Date(stream.createdAt+(state.task.input as {startFrame:number}).startFrame/stream.spec.fps*1000).toISOString()}`,
      `#EXT-X-MAP:URI="${path}?part=init"`,
      `#EXTINF:${((state.task.input as { frameCount: number }).frameCount / stream.spec.fps).toFixed(6)},`,
      `${path}?part=media`,
    );
  }
  if (stream.status === "stopped") lines.push("#EXT-X-ENDLIST");
  return lines.join("\n") + "\n";
}

/** Split a worker's fMP4 without decoding or re-encoding any frame. */
export function splitMp4(bytes: Uint8Array) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const init: Uint8Array[] = [],
    media: Uint8Array[] = [];
  let hasMoov = false,
    hasMoof = false,
    hasMdat = false;
  for (let offset = 0; offset < bytes.length;) {
    if (offset + 8 > bytes.length) throw Error("Truncated MP4 box");
    let size = view.getUint32(offset);
    const type = new TextDecoder().decode(
      bytes.subarray(offset + 4, offset + 8),
    );
    if (size === 1) {
      if (offset + 16 > bytes.length) throw Error("Truncated extended MP4 box");
      size = Number(view.getBigUint64(offset + 8));
    } else if (size === 0) size = bytes.length - offset;
    if (!Number.isSafeInteger(size) || size < 8 || offset + size > bytes.length)
      throw Error("Invalid MP4 box size");
    if (type === "ftyp" || type === "moov") {
      init.push(bytes.subarray(offset, offset + size));
      hasMoov ||= type === "moov";
    }
    if (type === "moof" || type === "mdat" || type === "styp") {
      media.push(bytes.subarray(offset, offset + size));
      hasMoof ||= type === "moof";
      hasMdat ||= type === "mdat";
    }
    offset += size;
  }
  if (!hasMoov || !hasMoof || !hasMdat)
    throw Error("Expected a fragmented MP4 with moov, moof and mdat");
  const concat = (parts: Uint8Array[]) => {
    const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
    let i = 0;
    for (const p of parts) {
      out.set(p, i);
      i += p.length;
    }
    return out;
  };
  return { init: concat(init), media: concat(media) };
}
