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

export type TaskCause = {
  streamId: string;
  epoch: number;
  segmentId?: string;
  startFrame?: number;
};

export type Task = {
  id: string;
  capability: string;
  input: JsonValue;
  cause?: TaskCause;
  affinityKey?: string;
  deadlineAt?: number;
  priority?: number;
};

export type TaskAssignment = {
  task: Task;
  attemptId: string;
  leaseUntil: number;
};

export type ShardSpec = {
  id: string;
  capability: string;
  input: JsonValue;
  epoch: number;
  startTick: number;
  affinityKey?: string;
};

export type ShardAssignment = {
  shard: ShardSpec;
  leaseId: string;
  leaseUntil: number;
  resumeTick: number;
  snapshotHash?: string;
  publication?: JsonValue;
};

export type ShardState = {
  shard: ShardSpec;
  status: "pending" | "leased" | "stopped";
  tick: number;
  workerId?: string;
  leaseId?: string;
  leaseUntil?: number;
  snapshotHash?: string;
  publication?: JsonValue;
  error?: string;
  createdAt: number;
  updatedAt: number;
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
    }
  | {
      type: "shard.heartbeat";
      workerId: string;
      shardId: string;
      leaseId: string;
      epoch: number;
      tick: number;
    }
  | {
      type: "shard.publish";
      shardId: string;
      leaseId: string;
      epoch: number;
      tick: number;
      snapshotHash?: string;
      output: JsonValue;
    }
  | {
      type: "shard.error";
      shardId: string;
      leaseId: string;
      epoch: number;
      tick: number;
      error: string;
    };

export type BrokerToWorker =
  | ({ type: "task.assign" } & TaskAssignment)
  | { type: "task.cancel"; taskId: string; attemptId: string }
  | ({ type: "shard.assign" } & ShardAssignment)
  | {
      type: "shard.revoke";
      shardId: string;
      leaseId: string;
      epoch: number;
      reason: string;
    };

export type TaskState = {
  task: Task;
  status: "pending" | "running" | "completed" | "failed" | "cancelled";
  workerId?: string;
  attemptId?: string;
  leaseUntil?: number;
  attempts: number;
  createdAt: number;
  updatedAt: number;
  output?: JsonValue;
  error?: string;
};

export type StreamSpec = {
  id: string;
  capability: string;
  width: number;
  height: number;
  fps: number;
  codec: string;
  segmentFrames: number;
  warmupFrames: number;
};

export type StreamEpoch = {
  streamId: string;
  id: number;
  startFrame: number;
  sceneHash: string;
  renderHash: string;
  snapshotHash?: string;
  createdAt: number;
};

export type StreamState = {
  spec: StreamSpec;
  currentEpoch: StreamEpoch;
  nextFrame: number;
  status: "active" | "stopped";
  createdAt: number;
  updatedAt: number;
};

export type RenderSegmentInput = {
  streamId: string;
  epoch: number;
  segmentId: string;
  startFrame: number;
  frameCount: number;
  warmupStartFrame: number;
  warmupFrames: number;
  width: number;
  height: number;
  fps: number;
  codec: string;
  sceneHash: string;
  renderHash: string;
  snapshotHash?: string;
};
