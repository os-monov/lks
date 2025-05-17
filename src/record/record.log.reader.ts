import { promises as fs } from 'fs';
import { FilePosition, PartitionId } from '../segment/types';
import { Record } from './record';
import { PartitionSegmentHeader } from '../segment/partition.segment.header';
import { PartitionSegmentPayloadItem } from '../segment/partition.segment.payload.item';

export class RecordLogReader {
  constructor(private readonly logFilePath: string) {}

  /**
   * Query the log file for the relevant records from the provided start position.
   * @param partitionId
   * @param position
   */
  public async query(
    partitionId: PartitionId,
    position: FilePosition,
  ): Promise<Record[]> {
    console.log(
      `[${new Date()}] Querying ${this.logFilePath} for partition: ${partitionId} starting at position: ${position}.`,
    );
    const results: Record[] = [];

    const file = await fs.open(this.logFilePath, 'r');
    const stats = await file.stat();
    console.log(
      `[${new Date()}] Size of ${this.logFilePath} is ${stats.size} bytes.`,
    );

    if (stats.size == 0) {
      await file.close();
      return results;
    }

    const magicBuf = Buffer.alloc(4);
    let currentPosition: FilePosition = position;

    while (currentPosition + PartitionSegmentHeader.SIZE <= stats.size) {
      await file.read(magicBuf, 0, 4, currentPosition);
      if (magicBuf.readUInt32BE(0) === PartitionSegmentHeader.MAGIC) {
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
        const payloadLength = header.getPayloadSize();
        const payloadBuffer = Buffer.alloc(payloadLength);
        await file.read(
          payloadBuffer,
          0,
          payloadLength,
          currentPosition + PartitionSegmentHeader.SIZE,
        );

        let payloadPosition = 0;
        let offset = header.getOffset();

        while (payloadPosition < payloadLength) {
          const keyLength = payloadBuffer.readUInt32BE(payloadPosition);
          payloadPosition += PartitionSegmentPayloadItem.LENGTH_SIZE;

          const key = payloadBuffer.toString(
            'utf8',
            payloadPosition,
            payloadPosition + keyLength,
          );
          payloadPosition += keyLength;

          const valueLength = payloadBuffer.readUInt32BE(payloadPosition);
          payloadPosition += PartitionSegmentPayloadItem.LENGTH_SIZE;

          const value = payloadBuffer.toString(
            'utf8',
            payloadPosition,
            payloadPosition + valueLength,
          );
          payloadPosition += valueLength;

          results.push(new Record(offset, key, value));
          offset += 1n;
        }
      }

      currentPosition += PartitionSegmentHeader.SIZE + header.getPayloadSize();
    }

    await file.close();
    return results;
  }
}
