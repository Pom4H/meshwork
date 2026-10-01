// Own one socket and one retry timer. Old socket events cannot cancel a newer
// connection or publish a stale assignment after reconnecting.
export function workerConnection(options) {
  const Socket = options.Socket ?? WebSocket;
  const timers = options.timers ?? globalThis;
  const now = options.now ?? Date.now;
  let current, retry, beat, stopped = false, failures = 0, openedAt = 0;
  let lastHeartbeat = -Infinity;
  const clear = () => {
    timers.clearTimeout(retry); retry = undefined;
    timers.clearInterval(beat); beat = undefined;
  };
  const send = (message) => {
    if (current?.readyState !== Socket.OPEN) return false;
    current.send(JSON.stringify(message));
    return true;
  };
  const heartbeat = (force = false) => {
    if (!force && now() - lastHeartbeat < 2000) return;
    if (send(options.heartbeat())) lastHeartbeat = now();
  };
  const connect = () => {
    if (stopped) return;
    clear();
    const socket = new Socket(options.url);
    current = socket;
    socket.addEventListener('open', () => {
      if (stopped || current !== socket) return;
      openedAt = now();
      send(options.hello());
      heartbeat(true);
      beat = timers.setInterval(() => heartbeat(true), 5000);
      options.onOpen?.();
    });
    socket.addEventListener('message', event => {
      if (!stopped && current === socket) options.onMessage?.(event);
    });
    socket.addEventListener('close', event => {
      if (stopped || current !== socket) return;
      clear(); current = undefined;
      options.onDisconnect?.(event);
      // Authentication/protocol errors require correction, not an endless loop.
      if (event.code === 1003 || event.code === 1008) {
        stopped = true; options.onFatal?.(event); return;
      }
      if (openedAt && now() - openedAt >= 30000) failures = 0;
      openedAt = 0;
      const delay = Math.min(30000, 1000 * 2 ** Math.min(failures++, 5));
      options.onRetry?.(delay);
      retry = timers.setTimeout(connect, delay);
    });
  };
  connect();
  return {
    send, heartbeat,
    resume() {
      if (!stopped && !current) connect();
      else if (current?.readyState === Socket.OPEN) heartbeat(true);
    },
    reconnect() { current?.close(4000, 'worker restarting'); },
    stop() { stopped = true; clear(); current?.close(); current = undefined; },
  };
}
