import { InternalServerException } from '../../src/exceptions';
import { Record } from '../../src/record/record';
import { PartitionSegment } from '../../src/segment/partition.segment';
import { PartitionSegmentHeader } from '../../src/segment/partition.segment.header';

describe('PartitionSegment', () => {
  const partitionId = 42;
  const startingOffset = 1000n;

  // Helper to create valid sequential records
  const initRecords = (count: number, startOffset = startingOffset): Record[] => {
    const records: Record[] = [];
    for (let i = 0; i < count; i++) {
      records.push(new Record(startOffset + BigInt(i), `key${i}`, `value${i}`));
    }
    return records;
  };

  describe('constructor', () => {
    it('should create a segment with valid records', () => {
      const records = initRecords(300);
      const segment = new PartitionSegment(partitionId, startingOffset, records);

      expect(segment.getPartitionId()).toBe(partitionId);
      expect(segment.getOffset()).toBe(startingOffset);
      expect(segment.getPayloadSize()).toBeGreaterThan(0);
    });

    it('should throw when records are not sequential', () => {
      const records = [
        new Record(1000n, 'key1', 'value1'),
        new Record(1001n, 'key2', 'value2'),
        new Record(1003n, 'key3', 'value3'), // Gap here
      ];

      expect(() => {
        new PartitionSegment(partitionId, 1000n, records);
      }).toThrow(InternalServerException);
    });

    it('should throw when first record offset does not match starting offset', () => {
      const records = initRecords(3, 1001n); // Starting at 1001

      expect(() => {
        new PartitionSegment(partitionId, 1000n, records); // But segment starts at 1000
      }).toThrow(InternalServerException);
    });

    it('should handle empty records array', () => {
      const segment = new PartitionSegment(partitionId, startingOffset, []);
      expect(segment.getPayloadSize()).toBe(0);
    });
  });

  describe('toBuffer', () => {
    it('should correctly serialize a segment', () => {
      const records = initRecords(2);
      const segment = new PartitionSegment(partitionId, startingOffset, records);
      const buffer = segment.toBuffer();

      // Should contain header + payload
      expect(buffer.length).toBe(PartitionSegmentHeader.SIZE + segment.getPayloadSize());

      // Header should have correct values
      const headerBuffer = buffer.subarray(0, PartitionSegmentHeader.SIZE);
      const header = PartitionSegmentHeader.from(headerBuffer);

      expect(header.getPartitionId()).toBe(partitionId);
      expect(header.getOffset()).toBe(startingOffset);
      expect(header.getPayloadLength()).toBe(segment.getPayloadSize());
    });
  });

  describe('from', () => {
    it('should correctly deserialize a buffer to a segment', () => {
      const records = initRecords(3);
      const segment = new PartitionSegment(partitionId, startingOffset, records);
      const buffer = segment.toBuffer();

      const deserialized = PartitionSegment.from(buffer);

      expect(deserialized.getPartitionId()).toBe(partitionId);
      expect(deserialized.getOffset()).toBe(startingOffset);
      expect(deserialized.getPayloadSize()).toBe(segment.getPayloadSize());
    });

    it('should correctly deserialize records with their content', () => {
      const records = [
        new Record(1000n, 'key0', 'value0'),
        new Record(1001n, 'key1', 'value1')
      ];

      const originalSegment = new PartitionSegment(partitionId, 1000n, records);
      const buffer = originalSegment.toBuffer();

      const deserializedSegment = PartitionSegment.from(buffer);
      const deserializedRecords = deserializedSegment.getRecords();

      expect(deserializedRecords.length).toBe(2);
      expect(deserializedRecords[0].getKey()).toBe('key0');
      expect(deserializedRecords[0].getValue()).toBe('value0');
      expect(deserializedRecords[1].getKey()).toBe('key1');
      expect(deserializedRecords[1].getValue()).toBe('value1');
    });
  });

  describe('round-trip', () => {
    it('should maintain data integrity through serialization and deserialization', () => {
      const testCases = [
        { key: 'simple', value: 'value' },
        { key: 'special chars: !@#$%^&*()', value: 'more special: ~`[]{}\\|;:\'",.<>/?' },
        { key: '你好', value: '世界' },  // Unicode characters
      ];

      const records = testCases.map((tc, i) =>
        new Record(startingOffset + BigInt(i), tc.key, tc.value)
      );

      const originalSegment = new PartitionSegment(partitionId, startingOffset, records);
      const buffer = originalSegment.toBuffer();
      const deserializedSegment = PartitionSegment.from(buffer);

      const originalRecords = originalSegment.getRecords();
      const deserializedRecords = deserializedSegment.getRecords();

      expect(deserializedRecords.length).toBe(originalRecords.length);

      for (let i = 0; i < originalRecords.length; i++) {
        expect(deserializedRecords[i].getOffset()).toBe(originalRecords[i].getOffset());
        expect(deserializedRecords[i].getKey()).toBe(originalRecords[i].getKey());
        expect(deserializedRecords[i].getValue()).toBe(originalRecords[i].getValue());
      }
    });
  });
});
