import * as fs from 'fs';
import { Record } from './record';
import { ControlPlaneService } from 'src/control.plane.service';
import { FilePosition, Offset, PartitionCommit, PartitionId } from './types';
import * as path from 'path';
import { BufferRecord } from './buffer.record';
import { InternalServerException } from 'src/exceptions';
import { PartitionSegment } from './partition.segment';
import { OnModuleDestroy } from '@nestjs/common';

export class RecordLog {
  private readonly dataDir: string;
  private readonly logFile: string;
  private readonly offsets: Map<PartitionId, Offset> = new Map();
  private activeBuffers: Map<PartitionId, BufferRecord[]> = new Map();
  private flushBuffers: Map<PartitionId, BufferRecord[]> = new Map();
  private isFlushing: boolean = false;
  private readonly flushInterval: NodeJS.Timeout;
  private FLUSH_INTERVAL_MS = 200;

  constructor(
    private readonly controlPlaneService: ControlPlaneService,
    private readonly index: number,
    private position: FilePosition,
    offsets: { partitionId: PartitionId; offset: Offset }[],
  ) {
    this.dataDir = `/tmp/lks`;
    this.logFile = path.join(this.dataDir, `${index}.log`);

    // Ensure data directory exists
    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir, { recursive: true });
    }

    // Create log file if it doesn't exist
    if (!fs.existsSync(this.logFile)) {
      fs.writeFileSync(this.logFile, '');
      console.log(`Created log file: ${this.logFile}`);
    }

    // Initialize partitions and their offsets
    for (const cursor of offsets) {
      this.offsets.set(cursor.partitionId, cursor.offset);
    }

    // set up periodic flush every 200ms
    this.flushInterval = setInterval(() => {
      this.flush().catch((err) => {
        console.error(`[RecordLog ${this.index}] Flush error:`, err);
      });
    }, this.FLUSH_INTERVAL_MS);
    console.log(
      `[RecordLog ${this.index}] Started flush interval (${this.FLUSH_INTERVAL_MS}ms)`,
    );
  }

  /**
   * Creates a {@link BufferRecord} and stores it in the corresponding partition buffer.
   * @param partitionId
   * @param key
   * @param value
   * @param callback
   */
  public async write(
    partitionId: PartitionId,
    key: string,
    value: string,
    callback: Function,
  ): Promise<void> {
    try {
      if (!this.offsets.has(partitionId)) {
        throw new InternalServerException();
      }

      const offset = this.offsets.get(partitionId);
      const bufferRecord = new BufferRecord(
        offset,
        key,
        value,
        new Date(),
        callback,
      );

      if (!this.activeBuffers.has(partitionId)) {
        this.activeBuffers.set(partitionId, []);
      }

      this.activeBuffers.get(partitionId).push(bufferRecord);
      this.offsets.set(partitionId, offset + 1);
    } catch (error) {
      console.error(
        `[RecordLog ${this.index}] Error in write (local offset) for partition ${partitionId}:`,
        error,
      );
      callback(
        error instanceof Error
          ? error
          : new Error('Failed to prepare record for write with local offset'),
      );
    }
  }

  /**
   * Flushes the active buffers to the underlying file.
   * @returns
   */
  private async flush() {
    if (this.isFlushing) {
      return;
    }

    this.flushBuffers = this.activeBuffers;
    this.activeBuffers = new Map();
    this.isFlushing = true;

    try {
      const commits: PartitionCommit[] = [];

      for (const partitionId of this.flushBuffers.keys()) {
        const records: BufferRecord[] = this.flushBuffers.get(partitionId);
        if (records.length === 0) {
          continue;
        }

        const segmentOffset = records[0].getOffset();
        const commit: PartitionCommit = {
          partitionId: partitionId,
          offset: segmentOffset,
          position: this.position,
        };
        commits.push(commit);
        const segment = new PartitionSegment(
          partitionId,
          segmentOffset,
          records,
        );
        const serialized = segment.serialize();
        await fs.promises.appendFile(this.logFile, serialized);
        this.position += Buffer.byteLength(serialized);
      }

      this.controlPlaneService.commit(commits);

      // how to handle failures, a.k.a. record.reject()
      for (const partitionId of this.flushBuffers.keys()) {
        const records: BufferRecord[] = this.flushBuffers.get(partitionId);
        for (const record of records) {
          record.resolve(record.getOffset());
        }
      }
    } catch (err) {
      for (const partitionId of this.flushBuffers.keys()) {
        const records: BufferRecord[] = this.flushBuffers.get(partitionId);
        for (const record of records) {
          record.reject(new InternalServerException());
          console.error(`Rejecting ${record.getOffset()}`);
        }
      }
    } finally {
      this.isFlushing = false;
      this.flushBuffers.clear();
    }
  }

  /**
   * Query the log file for the relevant records.
   * @param partitionId
   * @param position
   * @returns
   */
  public async query(
    partitionId: PartitionId,
    position: FilePosition,
  ): Promise<Record[]> {
    if (this.position === 0) {
      return [];
    }

    // segmentHeader =

    // fs.createReadStream(this.logFile, { start,  end })
    // const data =
  }

  // async onDestroy() {
  //     clearInterval(this.flushInterval);
  //     await this.flush();
  //     console.log(`[RecordLog ${this.index}] Flushed on destroy`);
  // }
}
