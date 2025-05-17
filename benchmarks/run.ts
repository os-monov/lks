import axios from 'axios';
import { faker } from "@faker-js/faker";
import * as http from "http";

const SERVER_URL = 'http://localhost:8123';
const PARTITIONS = 100;
const TPS_PER_PARTITION = 20;
const QUERY_INTERVAL_MS = 1000;

interface RecordKV {
  key: string;
  value: string;
}

const httpAgent = new http.Agent({
  keepAlive: true,
  maxSockets: 200,
  maxFreeSockets: 200,
})

const api = axios.create({
  baseURL: SERVER_URL,
  httpAgent,
  headers: { 'Content-Type': 'application/json' }
});

class PartitionProducer {
  public produced: RecordKV[] = [];
  private timer: NodeJS.Timeout | null = null;

  constructor(public partitionId: number, private tps: number) { }

  start() {
    const interval = 1000 / this.tps;
    this.timer = setInterval(() => this.produce(), interval);
  }

  stop() {
    if (this.timer) clearInterval(this.timer);
  }

  async produce() {
    const payload = {
      key: faker.finance.accountNumber(),
      value: faker.person.fullName(),
    };
    try {
      await api.post(`/produce/${this.partitionId}`, payload);

      this.produced.push(payload);
      if (this.produced.length % 10 === 0) {
        console.log(`[partition ${this.partitionId}] Produced ${this.produced.length} records`);
      }
    } catch (err) {
      console.error(`[partition ${this.partitionId}] Failed to send record: ${err}`);
    }
  }
}

class BenchmarkRunner {
  private producers: PartitionProducer[];
  private queryTimers: NodeJS.Timeout[] = [];

  constructor(
    private partitions = 10,
    private tpsPerPartition = 100,
    private queryIntervalMs = 1000
  ) {
    this.producers = [];
  }

  start() {
    for (let i = 0; i < this.partitions; i++) {
      const producer = new PartitionProducer(i, this.tpsPerPartition);
      producer.start();
      this.producers.push(producer);
    }

    for (let i = 0; i < this.partitions; i++) {
      const partitionId = i;
      const initialDelay = Math.random() * this.queryIntervalMs;

      setTimeout(() => {
        this.validatePartition(partitionId);
        const timerId = setInterval(() => {
          this.validatePartition(partitionId);
        }, this.queryIntervalMs);
        this.queryTimers.push(timerId);
      }, initialDelay);
    }

    console.log(
      `Started ${this.partitions} producers at ${this.tpsPerPartition} TPS each. ` +
      `Querying each of ${this.partitions} partitions approx. every ${this.queryIntervalMs}ms with staggered starts.`
    );
  }

  stop() {
    this.producers.forEach((p) => p.stop());
    this.queryTimers.forEach((timerId) => clearInterval(timerId));
    this.queryTimers = [];
    console.log('Stopped producers and partition query timers.');
  }

  async validatePartition(partitionId: number) {
    try {
      const startTime = Date.now();
      const response = await api.get(`/fetch/${partitionId}`);
      const endTime = Date.now();
      const latency = endTime - startTime;

      const fetched: RecordKV[] = response.data;

      const producer = this.producers[partitionId];
      if (!producer) {
        console.warn(`[VALIDATION] Partition ${partitionId}: No producer found. Latency: ${latency}ms`);
        return;
      }
      const produced = producer.produced;

      const N = Math.min(10, produced.length, fetched.length);
      let passed = true;
      if (N === 0 && (produced.length > 0 || fetched.length > 0)) {
        passed = false;
      } else if (N > 0) {
        for (let i = 1; i <= N; i++) {
          const prod = produced[produced.length - i];
          const rec = fetched[fetched.length - i];
          if (!rec || !prod || rec.key !== prod.key || rec.value !== prod.value) {
            passed = false;
            break;
          }
        }
      }

      if (passed) {
        console.log(`[VALIDATION] Partition ${partitionId}: ✅ Last ${N} records match. Latency: ${latency}ms`);
      } else {
        console.warn(`[VALIDATION] Partition ${partitionId}: ❌ Mismatch in last ${N} records (fetched: ${fetched.length}, produced: ${produced.length}). Latency: ${latency}ms`);
      }
    } catch (err: any) {
      const errorMessage = err.message || err;
      console.error(`[VALIDATION] Error querying partition ${partitionId}: ${errorMessage}. Latency: N/A`);
    }
  }
}

const runner = new BenchmarkRunner(PARTITIONS, TPS_PER_PARTITION, QUERY_INTERVAL_MS);
runner.start();

process.on('SIGINT', () => {
  console.log('Shutting down...');
  runner.stop();
  process.exit();
});
