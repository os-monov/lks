import * as fs from 'fs';
import {
  FilePosition,
  Offset,
  PartitionCommit,
  PartitionId,
} from '../segment/types';
import { BufferRecord } from './buffer.record';
import { InternalServerException } from 'src/exceptions';
import { Subject } from 'rxjs';
import { bufferTime } from 'rxjs';
import { groupBy } from 'lodash';
import { PartitionSegment } from 'src/segment/partition.segment';
import { ControlPlaneService } from 'src/control.plane.service';
import { MetricsService } from 'src/metrics.service';
import { ConsoleLogger } from 'src/console.logger';

export interface RecordLogWriterConfiguration {
  logFilePath: string;
  position: FilePosition;
  offsets: Map<PartitionId, Offset>;
}

/**
 * Responsible for batching records and then saving them to a file.
 */
export class RecordLogWriter {
  private static readonly BATCH_DURATION_MS = 250;
  private bufferSubject = new Subject<BufferRecord>();
  private readonly offsets: Map<PartitionId, Offset>;

  constructor(
    private readonly logFilePath: string,
    private position: FilePosition,
    offsets: Map<PartitionId, Offset>,
    private readonly controlPlaneService: ControlPlaneService,
    private readonly metricsService: MetricsService,
    private readonly logger: ConsoleLogger,
  ) {
    this.offsets = new Map(offsets);

    if (!fs.existsSync(this.logFilePath)) {
      fs.writeFileSync(this.logFilePath, '');
      this.logger.info(`[Writer @ ${this.logFilePath}] Created new log file.`);
    }

    /* Initialize the buffer subject */
    this.bufferSubject
      .pipe(bufferTime(RecordLogWriter.BATCH_DURATION_MS))
      .subscribe((records) => {
        if (records.length > 0) {
          this.flush(records);
        }
      });
  }

  /**
   * Write key-value record to corresponding partition.
   * @param partitionId
   * @param key
   * @param value
   * @returns
   */
  public async write(
    partitionId: PartitionId,
    key: string,
    value: string,
  ): Promise<Offset> {
    if (!this.offsets.has(partitionId)) {
      this.logger.error(
        `InternalServiceException: Offset not found for partition: ${partitionId}.`,
      );
      throw new InternalServerException();
    }
    const offset = this.offsets.get(partitionId);
    this.offsets.set(partitionId, offset + 1n);

    const bufferRecord = new BufferRecord(partitionId, offset, key, value);
    this.bufferSubject.next(bufferRecord);

    return bufferRecord.promise;
  }

  /**
   * Convert records into {@link PartitionSegment} and append them to the underylying file.
   * @param records
   */
  private async flush(records: BufferRecord[]): Promise<void> {
    const flushStart = Date.now();
    this.logger.info(
      `[Writer @ ${this.logFilePath}] Flushing ${records.length} records to file.`,
    );

    try {
      const recordsByPartition: Record<PartitionId, BufferRecord[]> = groupBy(
        records,
        (r: BufferRecord) => r.getPartitionId(),
      );
      const commits: PartitionCommit[] = [];
      const buffers: Buffer[] = [];
      for (const [partitionId, bufferRecords] of Object.entries(
        recordsByPartition,
      )) {
        if (!bufferRecords.length) continue;

        bufferRecords.sort((a, b) => Number(a.getOffset() - b.getOffset()));

        const segmentOffset = bufferRecords[0].getOffset();
        const segment = new PartitionSegment(
          Number(partitionId),
          segmentOffset,
          bufferRecords,
        );
        const segmentBuffer = segment.toBuffer();

        buffers.push(segmentBuffer);

        commits.push({
          partitionId: Number(partitionId),
          offset: segmentOffset,
          position: this.position,
        });

        // ensure position is correct
        this.position += segmentBuffer.length;
      }

      if (buffers.length > 0) {
        const concatenated = Buffer.concat(buffers);

        const syncStart = Date.now();
        fs.appendFileSync(this.logFilePath, concatenated);
        this.metricsService.emit('writer.flush.sync', Date.now() - syncStart);
        this.metricsService.emit('writer.record.count', records.length);
        this.metricsService.emit('writer.buffer.size', concatenated.length);
      }

      this.controlPlaneService.saveCommits(commits);
      this.logger.info(
        `[Writer @ ${this.logFilePath}] Commited ${commits.length} partitions to ControlPlaneService.`,
      );

      records.forEach((record) => record.resolve(record.getOffset()));
    } catch (error) {
      this.logger.error(error);
      records.forEach((record) => record.reject(new InternalServerException()));
      this.logger.info(
        `[Writer @ ${this.logFilePath}] Error flushing records.`,
      );
    }
    this.metricsService.emit('writer.flush', Date.now() - flushStart);
  }
}
