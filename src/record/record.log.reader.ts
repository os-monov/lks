import { promises as fs } from 'fs';
import { FilePosition, PartitionId } from '../segment/types';
import { Record } from './record';
import { PartitionSegmentHeader } from '../segment/partition.segment.header';
import { PartitionSegment } from '../segment/partition.segment';
import { ConsoleLogger } from 'src/console.logger';
import { MetricsService } from 'src/metrics.service';

export class RecordLogReader {
  constructor(
    private readonly logFilePath: string,
    private readonly metricsService: MetricsService,
    private readonly logger: ConsoleLogger,
  ) {}

  /**
   * Query the log file for the relevant records from the provided start position.
   * @param partitionId
   * @param position
   */
  public async query(
    partitionId: PartitionId,
    position: FilePosition,
  ): Promise<Record[]> {
    const start = Date.now();
    this.logger.info(
      `[Reader @ ${this.logFilePath}] Querying partition ${partitionId} from position: ${position}.`,
    );

    const results: Record[] = [];
    const file = await fs.open(this.logFilePath, 'r');
    const stats = await file.stat();
    this.logger.info(
      `[Reader @ ${this.logFilePath}] Log file is ${stats.size} bytes.`,
    );

    if (stats.size == 0) {
      await file.close();
      return results;
    }

    const magicBuffer = Buffer.alloc(4);
    let currentPosition: FilePosition = position;

    while (currentPosition + PartitionSegmentHeader.SIZE <= stats.size) {
      await file.read(magicBuffer, 0, 4, currentPosition);
      if (magicBuffer.readUInt32BE(0) === PartitionSegmentHeader.MAGIC) {
        break;
      }
      currentPosition += 1;
    }

    if (currentPosition + PartitionSegmentHeader.SIZE > stats.size) {
      await file.close();
      return results;
    }

    const headerBuffer = Buffer.alloc(PartitionSegmentHeader.SIZE);
    while (currentPosition + PartitionSegmentHeader.SIZE <= stats.size) {
      await file.read(
        headerBuffer,
        0,
        PartitionSegmentHeader.SIZE,
        currentPosition,
      );
      const header = PartitionSegmentHeader.from(headerBuffer);

      if (header.getPartitionId() === partitionId) {
        const segmentSize =
          PartitionSegmentHeader.SIZE + header.getPayloadSize();
        const segmentBuffer = Buffer.alloc(segmentSize);

        // read into segment buffer
        await file.read(segmentBuffer, 0, segmentSize, currentPosition);

        // create partition segment
        const segment = PartitionSegment.from(segmentBuffer);

        // append results
        results.push(...segment.getRecords());
      }

      currentPosition += PartitionSegmentHeader.SIZE + header.getPayloadSize();
    }

    this.metricsService.emit('reader.query', Date.now() - start);
    this.metricsService.emit('reader.count', results.length);
    await file.close();
    return results;
  }
}
