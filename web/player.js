import Hls from "/vendor/hls.js";
const id = location.pathname.split("/").at(-1),
  video = document.querySelector("video");
const status = document.querySelector("#state"),
  error = document.querySelector("#error");
let epoch,
  started = false,
  hls;
const sound=document.querySelector('#sound');
window.meshworkSourceTime=()=>{
 const stream=window.meshworkStream;
 if(!stream)return video.currentTime;
 if(hls?.playingDate)return (hls.playingDate.getTime()-stream.createdAt)/1000;
 const start=video.getStartDate?.();
 if(start instanceof Date&&Number.isFinite(start.getTime()))return (start.getTime()-stream.createdAt)/1000+video.currentTime;
 return video.currentTime+(stream.playbackStartFrame??stream.currentEpoch.startFrame)/stream.spec.fps;
};
function soundState(){sound.textContent=video.muted?'Включить звук':'Выключить звук';sound.setAttribute('aria-pressed',String(!video.muted));}
sound.addEventListener('click',()=>{
  video.muted=!video.muted;
  if(!video.muted){video.volume=.8;video.play().catch(()=>{error.textContent='Нажми ▶, чтобы начать воспроизведение со звуком.';});}
  soundState();
});
video.addEventListener('volumechange',soundState);
async function update() {
  try {
    const response = await fetch(`/streams/${id}`);
    if (!response.ok) throw Error(await response.text());
    const stream = await response.json();
    window.meshworkStream=stream;
    const [tasks, workers] = await Promise.all([
      fetch(`/streams/${id}/segments?epoch=${stream.currentEpoch.id}`).then(
        (r) => r.json(),
      ),
      fetch("/workers").then((r) => r.json()),
    ]);
    const completed = tasks.filter((t) => t.status === "completed").length;
    const compatible = workers.filter(
      (w) =>
        w.capabilities.includes(stream.spec.capability) &&
        (!stream.sceneChanges?.length||w.capabilities.includes('scene.live-controls.v1')) &&
        w.videoCodecs?.includes(stream.spec.codec),
    );
    status.textContent = `${stream.spec.width} × ${stream.spec.height} · цель ${stream.spec.fps} fps · воркеров ${compatible.length} · сегментов готово ${completed}`;
    error.textContent =
      stream.error ?? tasks.find((t) => t.status === "failed")?.error ?? "";
    if(stream.sceneChanges?.length&&workers.length&&!compatible.length)error.textContent='Обновите вкладки воркеров, чтобы они поддерживали изменения сцены из чата.';
    if (epoch !== stream.currentEpoch.id) {
      epoch = stream.currentEpoch.id;
      started = false;
      hls?.destroy();
      video.removeAttribute("src");
      video.load();
    }
    if (
      !started &&
      tasks.some(
        (t) =>
          t.status === "completed" &&
          t.task.cause.startFrame ===
            (stream.playbackStartFrame ?? stream.currentEpoch.startFrame),
      )
    ) {
      started = true;
      const url = `/streams/${id}/playlist.m3u8?epoch=${epoch}`;
      if (video.canPlayType("application/vnd.apple.mpegurl")) video.src = url;
      else if (Hls.isSupported()) {
        hls = new Hls({ liveSyncDurationCount: 3 });
        window.meshworkHls=hls;
        window.meshworkHlsErrors=[];
        hls.on(Hls.Events.ERROR, (_, data) => {
          window.meshworkHlsErrors.push({details:data.details,reason:data.reason,fatal:data.fatal});
          window.meshworkHlsErrors=window.meshworkHlsErrors.slice(-10);
          if (data.fatal) {
            error.textContent = data.details;
            hls.destroy();
            started = false;
          }
        });
        hls.loadSource(url);
        hls.attachMedia(video);
      } else throw Error("Этот браузер не поддерживает HLS-видео");
      video.play().catch(() => {
        status.textContent += " · нажмите ▶";
      });
    }
  } catch (e) {
    error.textContent = e.message;
  }
}
void update();
setInterval(update, 2000);
