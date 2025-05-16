import { Record } from './record';
import { Offset, PartitionId } from './types';

export class PartitionSegment {
  private readonly length: number;

  constructor(
    private readonly partitionId: PartitionId,
    private readonly offset: Offset,
    private readonly payload: Record[],
  ) {
    // Calculate length during construction
    const serializedRecords = this.serializeRecords();
    this.length = Buffer.byteLength(serializedRecords);
  }

  public getPartitionId(): PartitionId {
    return this.partitionId;
  }

  public getOffset(): Offset {
    return this.offset;
  }

  public getLength(): number {
    return this.length;
  }

  public getPayload(): Record[] {
    return this.payload;
  }

  /**
   * Helper method to serialize just the records part
   * We only store key:value pairs since offsets can be calculated from base offset
   */
  private serializeRecords(): string {
    // Serialize each record without the offset (we'll calculate it during deserialization)
    const recordStrings = this.payload.map(
      (record) => `${record.getKey()}:${record.getValue()}`,
    );

    return recordStrings.join('|');
  }

  /**
   * Serialize the segment to a compact string format.
   * Format: partitionId|baseOffset|recordCount|byteLength|key1:value1|key2:value2|...
   */
  public serialize(): string {
    // Serialize records once
    const serializedRecords = this.serializeRecords();

    // Create header with segment metadata
    const recordCount = this.payload.length;
    const header = `${this.partitionId}|${this.offset}|${recordCount}|${this.length}`;

    // Join header and records with a delimiter
    return `${header}|${serializedRecords}`;
  }

  /**
   * Deserialize a string back into a PartitionSegment.
   * @param serialized The serialized string
   * @returns A new PartitionSegment instance
   */
  public static deserialize(serialized: string): PartitionSegment {
    const parts = serialized.split('|');

    // Parse header
    const partitionId = parseInt(parts[0], 10);
    const baseOffset = parseInt(parts[1], 10);
    const recordCount = parseInt(parts[2], 10);
    const byteLength = parseInt(parts[3], 10);

    // Extract and parse records - calculate offsets from base
    const records: Record[] = [];
    for (let i = 0; i < recordCount; i++) {
      const recordIndex = i + 4; // Skip the 4 header parts
      if (recordIndex < parts.length) {
        const recordParts = parts[recordIndex].split(':');
        if (recordParts.length >= 2) {
          const key = recordParts[0];
          const value = recordParts[1];

          // Calculate offset based on position in segment
          const recordOffset = baseOffset + i;

          records.push(new Record(recordOffset, key, value));
        }
      }
    }

    return new PartitionSegment(partitionId, baseOffset, records);
  }

  /**
   * Calculate the total serialized length of the segment
   */
  public getTotalLength(): number {
    const serialized = this.serialize();
    return Buffer.byteLength(serialized);
  }
}
