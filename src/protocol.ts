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

export type TaskAssignment = {
  task: Task;
  attemptId: string;
  leaseUntil: number;
};

export type WorkerToBroker =
  | { type: "worker.hello"; worker: WorkerDescriptor }
  | {
      type: "worker.heartbeat";
      workerId: string;
      taskId?: string;
      attemptId?: string;
    }
  | {
      type: "task.result";
      taskId: string;
      attemptId: string;
      output: JsonValue;
    }
  | {
      type: "task.error";
      taskId: string;
      attemptId: string;
      error: string;
    };

export type BrokerToWorker =
  | ({ type: "task.assign" } & TaskAssignment)
  | { type: "task.cancel"; taskId: string; attemptId: string };

export type TaskState = {
  task: Task;
  status: "pending" | "running" | "completed" | "failed";
  workerId?: string;
  attemptId?: string;
  leaseUntil?: number;
  attempts: number;
  createdAt: number;
  updatedAt: number;
  output?: JsonValue;
  error?: string;
};
