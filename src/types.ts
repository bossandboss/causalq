export type JobState = 'pending' | 'reserved' | 'running' | 'done' | 'failed' | 'dead';

export interface PriorityRule {
  id: string;
  context: string;
  type: 'boost' | 'starvation_boost' | 'context_outranks_age';
  value: number;
  targetClass: string;
}

export interface Job {
  id: string;
  contentHash: string;
  payload: string;
  context: string;
  state: JobState;
  priorityScore: number;
  basePriority: number;
  maxRetries: number;
  retriesCount: number;
  lastError?: string;
  createdAt: number; // timestamp ms
  runAt: number;     // timestamp ms
  reservedAt?: number;
  completedAt?: number;
  predictedTime: number; // in seconds
  realCpuTime: number;   // in seconds
  isAnomaly: boolean;
}

export interface WorkerState {
  id: number;
  busy: boolean;
  activeJobId?: string;
  activeJobContext?: string;
  cpuLoad: number;  // percentage (0-100)
  memUsage: number; // percentage (0-100)
}

export interface QueueMetrics {
  totalEnqueued: number;
  completedCount: number;
  failedCount: number;
  deadCount: number;
  backlogCount: number;
  anomalyCount: number;
  averageCpuTimes: Record<string, number>;
}
