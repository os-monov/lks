import axios from 'axios';
import { faker } from "@faker-js/faker";
import * as http from "http";

const SERVER_URL = 'http://localhost:8123';
const PARTITIONS = 20;
const TPS_PER_PARTITION = 50;
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

class TestHarness {
  private producers: PartitionProducer[] = [];
  private queryTimer: NodeJS.Timeout | null = null;

  constructor(
    private partitions: number,
    private tpsPerPartition: number,
    private queryIntervalMs: number
  ) { }

  start() {
    // Start producers
    for (let i = 0; i < this.partitions; i++) {
      const producer = new PartitionProducer(i, this.tpsPerPartition);
      producer.start();
      this.producers.push(producer);
    }
    // Start periodic queries
    this.queryTimer = setInterval(() => this.queryRandomPartition(), this.queryIntervalMs);
    console.log(
      `Started ${this.partitions} producers at ${this.tpsPerPartition} TPS each. Querying random partition every ${this.queryIntervalMs}ms`
    );
  }

  stop() {
    this.producers.forEach((p) => p.stop());
    if (this.queryTimer) clearInterval(this.queryTimer);
  }

  async queryRandomPartition() {
    const partitionId = Math.floor(Math.random() * this.partitions);
    await this.validatePartition(partitionId);
  }

  async validatePartition(partitionId: number) {
    try {
      const response = await api.get(`/fetch/${partitionId}`);

      const fetched: RecordKV[] = response.data;
      const produced = this.producers[partitionId].produced;

      const N = Math.min(10, produced.length, fetched.length);
      let passed = true;
      for (let i = 1; i <= N; i++) {
        const prod = produced[produced.length - i];
        const rec = fetched[fetched.length - i];
        if (!rec || rec.key !== prod.key || rec.value !== prod.value) {
          passed = false;
          break;
        }
      }
      if (passed) {
        console.log(`[VALIDATION] Partition ${partitionId}: ✅ Last ${N} records match.`);
      } else {
        console.warn(`[VALIDATION] Partition ${partitionId}: ❌ Mismatch in last ${N} records!`);
      }
    } catch (err) {
      console.error(`[VALIDATION] Error querying partition ${partitionId}: ${err}`);
    }
  }
}

// Run the test harness
const harness = new TestHarness(PARTITIONS, TPS_PER_PARTITION, QUERY_INTERVAL_MS);
harness.start();

// Optional: handle shutdown/cleanup
process.on('SIGINT', () => {
  console.log('Shutting down...');
  harness.stop();
  process.exit();
});
