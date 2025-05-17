import { Injectable } from '@nestjs/common';
import * as path from 'path';
import * as fs from 'fs';
import { bufferTime, Subject } from 'rxjs';
import { Cron, CronExpression } from '@nestjs/schedule';

export interface Metric {
  timestamp: number;
  name: string;
  value: number;
}

@Injectable()
export class MetricsService {
  private readonly baseDirectory: string = '/tmp/lks';
  private readonly metricsFilePath = path.join(
    this.baseDirectory,
    'metrics.csv',
  );
  private metricsSubject = new Subject<Metric>();

  constructor() {
    // empty metrics file
    if (!fs.existsSync(this.baseDirectory)) {
      fs.mkdirSync(this.baseDirectory, { recursive: true });
    }
    // empty metrics file and write header
    fs.writeFileSync(this.metricsFilePath, 'timestamp,metric,value\n');

    this.metricsSubject.pipe(bufferTime(500)).subscribe((metrics) => {
      if (metrics.length > 0) {
        // Only flush if there are metrics
        this.flush(metrics);
      }
    });
  }

  public emit(name: string, value: number): void {
    this.metricsSubject.next({
      timestamp: Date.now(),
      name: name,
      value: value,
    });
  }

  @Cron(CronExpression.EVERY_10_SECONDS)
  private emitProcessMetrics(): void {
    const cpuUsage = process.cpuUsage();
    this.emit('server.cpu.user', cpuUsage.user);
    this.emit('server.cpu.system', cpuUsage.system);

    const memoryUsage = process.memoryUsage();
    this.emit('server.memory.rss', memoryUsage.rss);
    this.emit('server.memory.heapTotal', memoryUsage.heapTotal);
    this.emit('server.memory.heapUsed', memoryUsage.heapUsed);
    this.emit('server.memory.external', memoryUsage.external);
  }

  private async flush(metrics: Metric[]): Promise<void> {
    if (!metrics || metrics.length === 0) {
      return;
    }

    const rows = metrics
      .map((metric) => {
        return `${metric.timestamp},${metric.name},${metric.value}\n`;
      })
      .join('');

    try {
      await fs.promises.appendFile(this.metricsFilePath, rows);
    } catch (error) {
      console.error('Failed to flush metrics:', error);
      // Handle error appropriately, e.g., retry, log to a different system, etc.
    }
  }
}
