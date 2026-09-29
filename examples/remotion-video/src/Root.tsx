import { Composition } from "remotion";
import { MeshworkVideo, type MeshworkVideoProps } from "./Video";

const defaultProps: MeshworkVideoProps = {
  title: "Meshwork",
  subtitle: "One task. Any capable device.",
  worker: "render worker",
};

export const RemotionRoot = () => (
  <Composition
    id="MeshworkDemo"
    component={MeshworkVideo}
    durationInFrames={180}
    fps={30}
    width={1920}
    height={1080}
    defaultProps={defaultProps}
  />
);
