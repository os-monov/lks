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
import { Utils } from 'src/utils';

export interface RecordLogWriterConfiguration {
  logFilePath: string;
  position: FilePosition;
  offsets: Map<PartitionId, Offset>;
}

/**
 * Responsible for btaching records and then saving them to a file.
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
  ) {
    this.offsets = new Map(offsets);

    if (!fs.existsSync(this.logFilePath)) {
      fs.writeFileSync(this.logFilePath, '');
      console.log(`Created log file: ${this.logFilePath}`);
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
    const start = Date.now()
    if (!this.offsets.has(partitionId)) {
      console.log(
        `InternalServiceException: Offset not found for partition: ${partitionId}`,
      );
      throw new InternalServerException();
    }
    const offset = this.offsets.get(partitionId);
    this.offsets.set(partitionId, offset + 1n);

    const bufferRecord = new BufferRecord(partitionId, offset, key, value);
    this.bufferSubject.next(bufferRecord);

    const end = Date.now()
    Utils.emit("writer.write", end - start)
    return bufferRecord.promise;
  }

  /**
   * Convert records into {@link PartitionSegment} and append them to the underylying file.
   * @param records
   */
  private async flush(records: BufferRecord[]): Promise<void> {
    const start = Date.now()
    console.log(
      `[${new Date()}] Flushing ${records.length} records to ${this.logFilePath}.`,
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

        buffers.push(segmentBuffer)

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

        const start = Date.now()
        fs.appendFileSync(this.logFilePath, concatenated)
        const end = Date.now();
        Utils.emit("writer.flush.append", end - start);
        Utils.emit("record.count", records.length);
        Utils.emit("buffer.size", concatenated.length);
      }

      this.controlPlaneService.commit(commits);
      console.log(
        `[${new Date()}] Committed ${commits.length} partitions to ${this.logFilePath}.`,
      );

      records.forEach((record) => record.resolve(record.getOffset()));
    } catch (error) {
      console.log(error);
      records.forEach((record) => record.reject(new InternalServerException()));
      console.error(`Error flushing records for ${this.logFilePath}`);
    }
    const end = Date.now()
    Utils.emit("writer.flush", end - start)
  }
}
