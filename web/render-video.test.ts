import { test, expect } from "bun:test";

// Exercise the browser module with the codec support boundary substituted.
// No GPU or desktop codec assumptions enter these compatibility tests.
const source = await Bun.file(new URL("./render-video.js", import.meta.url)).text();
function moduleWith(canEncodeVideo: (codec: string, options: any) => Promise<boolean>) {
  const body = source.replace(/^import\s*\{[\s\S]*?\}\s*from\s*"\/vendor\/mediabunny.js";/, "const { Quality, canEncodeVideo } = dependencies;")
    .replace(/import \{applyLiveScene\} from '\/live-scene.js';/, 'const applyLiveScene=()=>{};')
    .replace(/export async function/g, "async function");
  class Quality { constructor(public options: any) {} }
  return new Function("dependencies", body + "\nreturn { videoCodecs, renderVideoSegment, advance };")({ Quality, canEncodeVideo });
}

test("advertise 720p codecs with hardware preference fallback and matching realtime settings", async () => {
  const previous = (globalThis as any).VideoEncoder;
  (globalThis as any).VideoEncoder = {};
  try {
    const calls: any[] = [];
    const module = moduleWith(async (codec, options) => {
      calls.push({ codec, ...options });
      return options.hardwareAcceleration === "no-preference";
    });
    expect(await module.videoCodecs()).toEqual(["avc", "hevc"]);
    expect(calls.map((c) => c.hardwareAcceleration)).toEqual([
      "prefer-hardware", "no-preference", "prefer-hardware", "no-preference",
    ]);
    for (const call of calls) {
      expect([call.width, call.height, call.frameRate]).toEqual([1280, 720, 60]);
      expect(call.latencyMode).toBe("realtime");
      expect(call.quality.options.bitrate).toBe(6635520);
    }
  } finally { (globalThis as any).VideoEncoder = previous; }
});

test('long timeline replay yields to socket events and reports cancellable progress',async()=>{
  const module=moduleWith(async()=>true);
  const controller=new AbortController();
  const channel=new MessageChannel();
  channel.port1.onmessage=()=>controller.abort();
  let slices=0, rendered=false;
  const runtime={tickRate:60,stepSeconds:1/60,scene:{tick:0,step(){this.tick++;},drainEvents(){},contacts:{drainFootsteps(){}}},
    renderer:{setTimelineFrame(){}},render(){rendered=true;},async drain(){}};
  await expect(module.advance(runtime,10000,60,controller.signal,undefined,()=>{
    slices++;if(slices===3)channel.port2.postMessage(null);
  })).rejects.toThrow('Task cancelled');
  expect(slices).toBeGreaterThanOrEqual(3);
  expect(runtime.scene.tick).toBeLessThan(10000);
  expect(rendered).toBe(false);
  channel.port1.close();channel.port2.close();
});

test("unsupported actual task codec reports dimensions and failure stage before loading GPU", async () => {
  const calls: any[] = [];
  const module = moduleWith(async (_, options) => { calls.push(options); return false; });
  await expect(module.renderVideoSegment(
    { codec: "avc", width: 1920, height: 1080, fps: 30, frameCount: 120 },
    null, new AbortController().signal,
  )).rejects.toThrow("check video encoder: avc encoding unavailable for 1920×1080 at 30 fps");
  expect(calls).toHaveLength(2);
  expect(calls.every((c) => c.width === 1920 && c.height === 1080 && c.frameRate === 30)).toBe(true);
});
