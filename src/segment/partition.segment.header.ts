import { Buffer } from 'buffer';
import { Offset, PartitionId } from './types';
import { InternalServerException } from '../exceptions';

/**
 * Defines the header section of a {@link PartitionSegment}.
 */
export class PartitionSegmentHeader {
  public static readonly MAGIC = 0xabcd1234;
  public static readonly SIZE = 20;

  /**
   * Immutable constructor.
   * @param partitionId
   * @param offset
   * @param payloadLength
   */
  constructor(
    private readonly partitionId: PartitionId,
    private readonly offset: Offset,
    private readonly payloadSize: number,
  ) {}

  /**
   * Create new instance from byte buffer.
   * @param buffer
   * @returns
   */
  static from(buffer: Buffer): PartitionSegmentHeader {
    const magic = buffer.readUInt32BE(0);
    if (magic !== PartitionSegmentHeader.MAGIC) {
      throw new InternalServerException();
    }
    const partitionId = buffer.readUInt32BE(4);
    const offset = buffer.readBigUInt64BE(8);
    const payloadSize = buffer.readUInt32BE(16);

    return new PartitionSegmentHeader(partitionId, offset, payloadSize);
  }

  /**
   * Create buffer from instance.
   * @returns
   */
  public toBuffer(): Buffer {
    const buffer = Buffer.alloc(PartitionSegmentHeader.SIZE);
    buffer.writeUInt32BE(PartitionSegmentHeader.MAGIC, 0);
    buffer.writeUInt32BE(this.partitionId, 4);
    buffer.writeBigUInt64BE(this.offset, 8);
    buffer.writeUInt32BE(this.payloadSize, 16);
    return buffer;
  }

  /**
   * Returns partition id.
   * @returns
   */
  public getPartitionId(): number {
    return this.partitionId;
  }

  /**
   * Returns starting offset for the segment.
   * @returns
   */
  public getOffset(): bigint {
    return this.offset;
  }

  /**
   * Returns the size of the payload.
   * @returns
   */
  public getPayloadSize(): number {
    return this.payloadSize;
  }
}
