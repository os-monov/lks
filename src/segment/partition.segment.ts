import { InternalServerException } from '../exceptions';
import { Record } from '../record/record';
import { PartitionSegmentHeader } from './partition.segment.header';
import { PartitionSegmentPayloadItem } from './partition.segment.payload.item';
import { Offset, PartitionId } from './types';

export class PartitionSegment {
  private readonly payloadSize: number;

  /**
   * Immutable constructor.
   * @param partitionId
   * @param offset
   * @param records
   */
  constructor(
    private readonly partitionId: PartitionId,
    private readonly offset: Offset,
    private readonly records: Record[],
  ) {
    // validate all records are sequential
    for (let i = 1; i < records.length; i++) {
      if (records[i].getOffset() !== records[i - 1].getOffset() + 1n) {
        console.log('Records must have sequential offsets.');
        throw new InternalServerException();
      }
    }

    // Validate first record offset matches starting offset
    if (records.length > 0 && records[0].getOffset() !== offset) {
      console.log('First record offset must match starting offset');
      throw new InternalServerException();
    }

    this.payloadSize = this.records.reduce(
      (sum, record) =>
        sum +
        Buffer.byteLength(record.getKey(), 'utf8') +
        Buffer.byteLength(record.getValue(), 'utf8') +
        2 * PartitionSegmentPayloadItem.LENGTH_SIZE,
      0,
    );
  }

  /**
   * Converts instance to a buffer.
   * @returns
   */
  toBuffer(): Buffer {
    const buffers: Buffer[] = [];
    for (const record of this.records) {
      const item = new PartitionSegmentPayloadItem(
        record.getKey(),
        record.getValue(),
      );
      buffers.push(item.toBuffer());
    }

    const payload = Buffer.concat(buffers);
    if (payload.length !== this.payloadSize) {
      throw new InternalServerException();
    }

    const header = new PartitionSegmentHeader(
      this.partitionId,
      this.offset,
      this.payloadSize,
    ).toBuffer();

    return Buffer.concat([header, payload]);
  }

  /**
   * Creates instance from a buffer.
   * @param buffer
   */
  static from(buffer: Buffer): PartitionSegment {
    const header = PartitionSegmentHeader.from(
      buffer.subarray(0, PartitionSegmentHeader.SIZE),
    );
    const payloadStart = PartitionSegmentHeader.SIZE;
    const payloadEnd = payloadStart + header.getPayloadSize();

    const records: Record[] = [];
    let position = payloadStart;
    let offset = header.getOffset();

    while (position < payloadEnd) {
      const keyLength = buffer.readUInt32BE(position);
      const valueLength = buffer.readUInt32BE(
        position + PartitionSegmentPayloadItem.LENGTH_SIZE + keyLength,
      );
      const itemSize =
        PartitionSegmentPayloadItem.LENGTH_SIZE +
        keyLength +
        PartitionSegmentPayloadItem.LENGTH_SIZE +
        valueLength;

      const itemBuffer = buffer.subarray(position, position + itemSize);
      const item = PartitionSegmentPayloadItem.from(itemBuffer);

      records.push(new Record(offset, item.getKey(), item.getValue()));

      position += itemSize;
      offset += 1n;
    }

    return new PartitionSegment(
      header.getPartitionId(),
      header.getOffset(),
      records,
    );
  }

  /**
   * Return partition id for the segment.
   * @returns
   */
  public getPartitionId(): PartitionId {
    return this.partitionId;
  }

  /**
   * Returns the starting offset for the segment.
   * @returns
   */
  public getOffset(): Offset {
    return this.offset;
  }

  /**
   * Returns the size of the payload.
   * @returns
   */
  public getPayloadSize(): number {
    return this.payloadSize;
  }

  /**
   * Returns the records.
   * @returns
   */
  public getRecords(): Record[] {
    return this.records;
  }
}
