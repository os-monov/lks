import { Buffer } from 'buffer';
import { PartitionSegmentHeader } from '../../src/segment/partition.segment.header';

describe('PartitionSegmentHeader', () => {
  const partitionId = 42;
  const offset = 9007199254740991n;
  const payloadLength = 12345;

  describe('constructor', () => {
    it('should create a header with provided values', () => {
      const header = new PartitionSegmentHeader(
        partitionId,
        offset,
        payloadLength,
      );

      expect(header.getPartitionId()).toBe(partitionId);
      expect(header.getOffset()).toBe(offset);
      expect(header.getPayloadLength()).toBe(payloadLength);
    });
  });

  describe('toBuffer', () => {
    it('should serialize to a buffer correctly', () => {
      const header = new PartitionSegmentHeader(
        partitionId,
        offset,
        payloadLength,
      );
      const buffer = header.toBuffer();

      expect(buffer.length).toBe(16); // 4 + 8 + 4 bytes
      expect(buffer.readUInt32BE(0)).toBe(partitionId);
      expect(buffer.readBigUInt64BE(4)).toBe(offset);
      expect(buffer.readUInt32BE(12)).toBe(payloadLength);
    });
  });

  describe('from', () => {
    it('should deserialize from a buffer correctly', () => {
      const buffer = Buffer.alloc(16);
      buffer.writeUInt32BE(partitionId, 0);
      buffer.writeBigUInt64BE(offset, 4);
      buffer.writeUInt32BE(payloadLength, 12);

      const header = PartitionSegmentHeader.from(buffer);

      expect(header.getPartitionId()).toBe(partitionId);
      expect(header.getOffset()).toBe(offset);
      expect(header.getPayloadLength()).toBe(payloadLength);
    });
  });

  describe('round-trip', () => {
    it('should maintain data integrity after serialization and deserialization', () => {
      const originalHeader = new PartitionSegmentHeader(
        partitionId,
        offset,
        payloadLength,
      );
      const buffer = originalHeader.toBuffer();
      const roundTripHeader = PartitionSegmentHeader.from(buffer);

      expect(roundTripHeader.getPartitionId()).toBe(
        originalHeader.getPartitionId(),
      );
      expect(roundTripHeader.getOffset()).toBe(originalHeader.getOffset());
      expect(roundTripHeader.getPayloadLength()).toBe(
        originalHeader.getPayloadLength(),
      );
    });
  });

  describe('edge cases', () => {
    it('should handle maximum values', () => {
      const maxPartitionId = 4294967295; // Max UInt32
      const maxOffset = 18446744073709551615n; // Max UInt64
      const maxPayloadLength = 4294967295; // Max UInt32

      const header = new PartitionSegmentHeader(
        maxPartitionId,
        maxOffset,
        maxPayloadLength,
      );
      const buffer = header.toBuffer();
      const roundTripHeader = PartitionSegmentHeader.from(buffer);

      expect(roundTripHeader.getPartitionId()).toBe(maxPartitionId);
      expect(roundTripHeader.getOffset()).toBe(maxOffset);
      expect(roundTripHeader.getPayloadLength()).toBe(maxPayloadLength);
    });

    it('should handle minimum values (zero)', () => {
      const minHeader = new PartitionSegmentHeader(0, 0n, 0);
      const buffer = minHeader.toBuffer();
      const roundTripHeader = PartitionSegmentHeader.from(buffer);

      expect(roundTripHeader.getPartitionId()).toBe(0);
      expect(roundTripHeader.getOffset()).toBe(0n);
      expect(roundTripHeader.getPayloadLength()).toBe(0);
    });
  });
});
