const autocannon = require('autocannon');
import axios from 'axios';
import { faker } from '@faker-js/faker';

interface BenchmarkerOptions {
  serverUrl: string;
  partitions: number;
  tps: number;
  fetchIntervalMs: number;
  runMinutes: number;
}

export class Benchmarker {
  private fetchTimers: NodeJS.Timeout[] = [];
  private running = false;

  constructor(private readonly opts: BenchmarkerOptions) { }

  async run() {
    this.running = true;
    this.startProducer();
    for (let partitionId = 0; partitionId < this.opts.partitions; partitionId++) {
      this.scheduleFetch(partitionId);
    }
    console.log(`Started Benchmarker for ${this.opts.runMinutes} minutes (${this.opts.partitions} partitions, ${this.opts.tps} TPS each).`);

    setTimeout(async () => {
      console.log('Benchmark complete. Stopping...');
      this.stop();
      process.exit(0);
    }, this.opts.runMinutes * 60 * 1000);
  }

  stop() {
    this.running = false;
    for (const timer of this.fetchTimers) clearTimeout(timer);
    this.fetchTimers = [];
    console.log('Stopped Benchmarker.');
  }

  private startProducer() {

    const { serverUrl, partitions, tps } = this.opts;
    const ac = autocannon({
      url: serverUrl,
      method: 'POST',
      connections: 300,
      pipelining: 1,
      duration: this.opts.runMinutes * 60,
      requests: [{
        method: 'POST',
        path: '',
        setupRequest: (req: any) => {
          const partitionId = Math.floor(Math.random() * partitions);
          req.path = `/produce/${partitionId}`;
          const key = faker.internet.email();
          const value = faker.finance.creditCardNumber();
          req.headers = { 'content-type': 'application/x-www-form-urlencoded' };
          req.body = `key=${encodeURIComponent(key)}&value=${encodeURIComponent(value)}`;
          return req;
        },
      }],
      overallRate: tps * partitions,
    });
    ac.on('done', () => console.log(`[autocannon] stopped.`));
  }

  private scheduleFetch(partitionId: number) {
    const fetchFn = async () => {
      if (!this.running) return;
      try {
        const url = `${this.opts.serverUrl}/fetch/${partitionId}`;
        await axios.get(url, { timeout: 30000 });

      } catch (err: any) {
        console.error(`[Fetch] Partition ${partitionId}: Error: ${err.message || err}`);
      }
      // Schedule next fetch with jitter
      if (this.running) {
        const base = this.opts.fetchIntervalMs; // e.g., 30000
        const jitter = Math.floor((Math.random() - 0.5) * 2 * base); // -base to +base
        const interval = base + jitter; // This is between 0 and 2*base (0-60s)
        this.fetchTimers[partitionId] = setTimeout(fetchFn, interval);
      }
    };
    // Initial jitter
    const base = this.opts.fetchIntervalMs; // e.g., 30000
    const jitter = Math.floor((Math.random() - 0.5) * 2 * base); // -base to +base
    const interval = base + jitter; // This is between 0 and 2*base (0-60s)
    this.fetchTimers[partitionId] = setTimeout(fetchFn, interval);
  }
}

// Example usage:
const opts: BenchmarkerOptions = {
  serverUrl: 'http://localhost:8123',
  partitions: 100,
  tps: 25,
  fetchIntervalMs: 30000,
  runMinutes: 10,
};
const bench = new Benchmarker(opts);
bench.run();

process.on('SIGINT', async () => {
  bench.stop();
  process.exit();
});