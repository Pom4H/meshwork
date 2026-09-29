import {
  AbsoluteFill,
  interpolate,
  spring,
  useCurrentFrame,
  useVideoConfig,
} from "remotion";

export type MeshworkVideoProps = {
  title: string;
  subtitle: string;
  worker: string;
};

const devices = ["iPad Pro", "Windows GPU", "Mac", "Android"];

export const MeshworkVideo = ({ title, subtitle, worker }: MeshworkVideoProps) => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const intro = spring({ frame, fps, config: { damping: 16 } });
  const fade = interpolate(frame, [140, 175], [1, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  return (
    <AbsoluteFill
      style={{
        background: "#090b10",
        color: "#f6f7fb",
        fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif",
        padding: 120,
        opacity: fade,
      }}
    >
      <div
        style={{
          fontSize: 26,
          letterSpacing: 4,
          textTransform: "uppercase",
          opacity: 0.55,
        }}
      >
        capability worker / {worker}
      </div>

      <div
        style={{
          marginTop: 120,
          fontSize: 132,
          fontWeight: 700,
          letterSpacing: -6,
          transform: `translateY(${(1 - intro) * 50}px)`,
          opacity: intro,
        }}
      >
        {title}
      </div>

      <div
        style={{
          marginTop: 24,
          fontSize: 44,
          opacity: 0.7 * intro,
        }}
      >
        {subtitle}
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(4, 1fr)",
          gap: 20,
          marginTop: 150,
        }}
      >
        {devices.map((device, index) => {
          const localFrame = Math.max(0, frame - 30 - index * 10);
          const progress = spring({ frame: localFrame, fps, config: { damping: 18 } });
          return (
            <div
              key={device}
              style={{
                border: "1px solid rgba(255,255,255,0.16)",
                borderRadius: 24,
                padding: 34,
                fontSize: 30,
                transform: `translateY(${(1 - progress) * 36}px)`,
                opacity: progress,
              }}
            >
              <div style={{ opacity: 0.45, fontSize: 18, marginBottom: 12 }}>
                worker {index + 1}
              </div>
              {device}
            </div>
          );
        })}
      </div>

      <div
        style={{
          position: "absolute",
          left: 120,
          right: 120,
          bottom: 80,
          display: "flex",
          justifyContent: "space-between",
          fontSize: 22,
          opacity: 0.42,
        }}
      >
        <span>video.remotion</span>
        <span>rendered by Meshwork</span>
      </div>
    </AbsoluteFill>
  );
};
