import { afterEach, describe, expect, test } from "bun:test";
import { rmSync } from "node:fs";
import { startBroker } from "./broker";
import type { BrokerToWorker, TaskState } from "./protocol";

const artifactDir = ".meshwork-test-artifacts";
let broker: ReturnType<typeof startBroker> | undefined;

afterEach(() => {
  broker?.stop(true);
  broker = undefined;
  rmSync(artifactDir, { recursive: true, force: true });
});

describe("broker", () => {
  test("assigns work and accepts only the active attempt", async () => {
    broker = startBroker({
      port: 0,
      artifactDir,
      leaseMs: 2_000,
      heartbeatTimeoutMs: 2_000,
    });
    const base = `http://127.0.0.1:${broker.port}`;
    const socket = new WebSocket(`ws://127.0.0.1:${broker.port}/ws`);

    await new Promise<void>((resolve, reject) => {
      socket.addEventListener("open", () => resolve(), { once: true });
      socket.addEventListener("error", () => reject(new Error("websocket failed")), {
        once: true,
      });
    });

    socket.send(JSON.stringify({
      type: "worker.hello",
      worker: {
        id: "test-worker",
        name: "test",
        platform: "bun",
        capabilities: ["echo.test"],
      },
    }));

    const assignmentPromise = new Promise<Extract<BrokerToWorker, { type: "task.assign" }>>(
      (resolve) => {
        socket.addEventListener("message", (event) => {
          const message = JSON.parse(String(event.data)) as BrokerToWorker;
          if (message.type === "task.assign") resolve(message);
        });
      },
    );

    const submittedResponse = await fetch(`${base}/tasks`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ capability: "echo.test", input: { ok: true } }),
    });
    expect(submittedResponse.status).toBe(202);

    const assignment = await assignmentPromise;

    socket.send(JSON.stringify({
      type: "task.result",
      taskId: assignment.task.id,
      attemptId: "stale-attempt",
      output: { bad: true },
    }));

    let state = await fetch(`${base}/tasks/${assignment.task.id}`).then(
      (response) => response.json() as Promise<TaskState>,
    );
    expect(state.status).toBe("running");

    socket.send(JSON.stringify({
      type: "task.result",
      taskId: assignment.task.id,
      attemptId: assignment.attemptId,
      output: { ok: true },
    }));

    for (let index = 0; index < 20; index += 1) {
      state = await fetch(`${base}/tasks/${assignment.task.id}`).then(
        (response) => response.json() as Promise<TaskState>,
      );
      if (state.status === "completed") break;
      await Bun.sleep(10);
    }

    expect(state.status).toBe("completed");
    expect(state.output).toEqual({ ok: true });
    socket.close();
  });
});
