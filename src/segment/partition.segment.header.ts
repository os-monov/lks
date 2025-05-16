import { Buffer } from 'buffer';
import { Offset, PartitionId } from './types';

/**
 * Defines the header section of a {@link PartitionSegment}.
 */
export class PartitionSegmentHeader {
  /* Defines the size of the header */
  private static readonly SIZE = 16;

  /**
   * Immutable constructor.
   * @param partitionId
   * @param offset
   * @param payloadLength
   */
  constructor(
    private readonly partitionId: PartitionId,
    private readonly offset: Offset,
    private readonly payloadLength: number,
  ) {}

  /**
   * Create new instance from byte buffer.
   * @param buffer
   * @returns
   */
  static from(buffer: Buffer): PartitionSegmentHeader {
    const partitionId = buffer.readUInt32BE(0);
    const offset = buffer.readBigUInt64BE(4);
    const payloadLength = buffer.readUint32BE(12);

    return new PartitionSegmentHeader(partitionId, offset, payloadLength);
  }

  /**
   * Create buffer from instance.
   * @returns
   */
  public toBuffer(): Buffer {
    const buffer = Buffer.alloc(PartitionSegmentHeader.SIZE);
    buffer.writeUint32BE(this.partitionId, 0);
    buffer.writeBigUInt64BE(this.offset, 4);
    buffer.writeUInt32BE(this.payloadLength, 12);
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
  public getPayloadLength(): number {
    return this.payloadLength;
  }
}
