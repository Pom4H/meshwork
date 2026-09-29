export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export type WorkerDescriptor = {
  id: string;
  name: string;
  platform: string;
  capabilities: string[];
};

export type Task = {
  id: string;
  capability: string;
  input: JsonValue;
};

export type WorkerToBroker =
  | { type: "worker.hello"; worker: WorkerDescriptor }
  | { type: "worker.heartbeat"; workerId: string }
  | { type: "task.result"; taskId: string; output: JsonValue }
  | { type: "task.error"; taskId: string; error: string };

export type BrokerToWorker =
  | { type: "task.assign"; task: Task }
  | { type: "task.cancel"; taskId: string };

export type TaskState = {
  task: Task;
  status: "pending" | "running" | "completed" | "failed";
  workerId?: string;
  output?: JsonValue;
  error?: string;
};
