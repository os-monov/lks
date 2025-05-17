import axios from 'axios';
import { faker } from "@faker-js/faker";
import * as http from "http";
const pLimit = require('p-limit');

const SERVER_URL = 'http://localhost:8123';
const PARTITIONS = 100;
const TPS_PER_PARTITION = 50;
const QUERY_INTERVAL_MS = 1000;
const VALIDATION_CONCURRENCY = 20; // Adjust as needed for your system

interface RecordKV {
  key: string;
  value: string;
}

const httpAgent = new http.Agent({
  keepAlive: true,
  maxSockets: 200,
  maxFreeSockets: 200,
});

const api = axios.create({
  baseURL: SERVER_URL,
  httpAgent,
  headers: { 'Content-Type': 'application/json' }
});

// ------------- PRODUCER ---------------
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

// ------------- BENCHMARK RUNNER ---------------
class BenchmarkRunner {
  private producers: PartitionProducer[];
  private validatorTimers: NodeJS.Timeout[] = [];
  private validationInFlight: boolean[] = [];
  private limitValidate: ReturnType<typeof pLimit>;

  constructor(
    private partitions = 10,
    private tpsPerPartition = 1,
    private queryIntervalMs = 1000,
    private validationConcurrency = 1
  ) {
    this.producers = [];
    this.limitValidate = pLimit(this.validationConcurrency);
    this.validationInFlight = Array(this.partitions).fill(false);
  }

  start() {
    for (let i = 0; i < this.partitions; i++) {
      const producer = new PartitionProducer(i, this.tpsPerPartition);
      producer.start();
      this.producers.push(producer);
    }

    // for (let i = 0; i < this.partitions; i++) {
    //   const partitionId = i;
    //   const initialDelay = Math.random() * this.queryIntervalMs;
    //   setTimeout(() => {
    //     this.runValidationLoop(partitionId);
    //   }, initialDelay);
    // }

    // console.log(
    //   `Started ${this.partitions} producers at ${this.tpsPerPartition} TPS each. ` +
    //   `Validating partitions every ~${this.queryIntervalMs}ms with concurrency limit ${this.validationConcurrency}.`
    // );
  }

  stop() {
    this.producers.forEach((p) => p.stop());
    // this.validatorTimers.forEach((timerId) => clearTimeout(timerId));
    // this.validatorTimers = [];
    console.log('Stopped producers and validation timers.');
  }

  private runValidationLoop(partitionId: number) {
    const validateAndSchedule = async () => {
      if (!this.validationInFlight[partitionId]) {
        this.validationInFlight[partitionId] = true;
        await this.limitValidate(() => this.validatePartition(partitionId));
        this.validationInFlight[partitionId] = false;
      }
      this.validatorTimers[partitionId] = setTimeout(validateAndSchedule, this.queryIntervalMs);
    };
    validateAndSchedule();
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

// --- Launch the test ---
const runner = new BenchmarkRunner(PARTITIONS, TPS_PER_PARTITION, QUERY_INTERVAL_MS, VALIDATION_CONCURRENCY);
runner.start();

process.on('SIGINT', () => {
  console.log('Shutting down...');
  runner.stop();
  process.exit();
});
