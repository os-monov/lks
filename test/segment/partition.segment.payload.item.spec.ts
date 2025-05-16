import { PartitionSegmentPayloadItem } from '../../src/segment/partition.segment.payload.item';
import { Buffer } from 'buffer';
import { InvalidRecordException } from '../../src/exceptions';

describe('PartitionSegmentPayloadItem', () => {
    const validKey = 'testKey';
    const validValue = 'testValue';

    describe('constructor', () => {
        it('should create a payload item with valid key and value', () => {
            const item = new PartitionSegmentPayloadItem(validKey, validValue);

            expect(item.getKey()).toBe(validKey);
            expect(item.getValue()).toBe(validValue);
        });

        it('should throw InvalidRecordException when key exceeds max size', () => {
            // Create a key that's over 1KB
            const largeKey = 'a'.repeat(1025);

            expect(() => {
                new PartitionSegmentPayloadItem(largeKey, validValue);
            }).toThrow(InvalidRecordException);
        });

        it('should throw InvalidRecordException when value exceeds max size', () => {
            // Create a value that's over 1KB
            const largeValue = 'a'.repeat(1025);

            expect(() => {
                new PartitionSegmentPayloadItem(validKey, largeValue);
            }).toThrow(InvalidRecordException);
        });
    });

    describe('toBuffer', () => {
        it('should serialize to buffer correctly', () => {
            const item = new PartitionSegmentPayloadItem(validKey, validValue);
            const buffer = item.toBuffer();

            // Length should be: keyLength(4) + key bytes + valueLength(4) + value bytes
            const expectedLength = 4 + Buffer.byteLength(validKey, 'utf8') + 4 + Buffer.byteLength(validValue, 'utf8');
            expect(buffer.length).toBe(expectedLength);

            // Check key length
            expect(buffer.readUInt32BE(0)).toBe(Buffer.byteLength(validKey, 'utf8'));

            // Check key content
            expect(buffer.toString('utf8', 4, 4 + Buffer.byteLength(validKey, 'utf8'))).toBe(validKey);

            // Check value length
            const keyByteLength = Buffer.byteLength(validKey, 'utf8');
            expect(buffer.readUInt32BE(4 + keyByteLength)).toBe(Buffer.byteLength(validValue, 'utf8'));

            // Check value content
            expect(buffer.toString('utf8', 8 + keyByteLength, 8 + keyByteLength + Buffer.byteLength(validValue, 'utf8'))).toBe(validValue);
        });
    });

    describe('from', () => {
        it('should deserialize from buffer correctly', () => {
            const originalItem = new PartitionSegmentPayloadItem(validKey, validValue);
            const buffer = originalItem.toBuffer();

            const deserialized = PartitionSegmentPayloadItem.from(buffer);

            expect(deserialized.getKey()).toBe(validKey);
            expect(deserialized.getValue()).toBe(validValue);
        });

        it('should handle empty strings', () => {
            const emptyItem = new PartitionSegmentPayloadItem('', '');
            const buffer = emptyItem.toBuffer();

            const deserialized = PartitionSegmentPayloadItem.from(buffer);

            expect(deserialized.getKey()).toBe('');
            expect(deserialized.getValue()).toBe('');
        });
    });

    describe('getSize', () => {
        it('should calculate size correctly', () => {
            const item = new PartitionSegmentPayloadItem(validKey, validValue);
            const buffer = item.toBuffer();

            expect(item.getSize()).toBe(buffer.length);
        });

        it('should match buffer length with various input sizes', () => {
            const testCases = [
                { key: 'a', value: 'b' },
                { key: 'abc', value: '123456789' },
                { key: 'test'.repeat(10), value: 'value'.repeat(20) }
            ];

            testCases.forEach(({ key, value }) => {
                const item = new PartitionSegmentPayloadItem(key, value);
                const buffer = item.toBuffer();

                expect(item.getSize()).toBe(buffer.length);
            });
        });
    });

    describe('round-trip', () => {
        it('should maintain data integrity through serialization and deserialization', () => {
            const testCases = [
                { key: 'simple', value: 'value' },
                { key: 'special chars: !@#$%^&*()', value: 'more special: ~`[]{}\\|;:\'",.<>/?' },
                { key: '你好', value: '世界' },  // Unicode characters
                { key: 'a'.repeat(1000), value: 'b'.repeat(1000) }  // Near max size
            ];

            testCases.forEach(({ key, value }) => {
                const original = new PartitionSegmentPayloadItem(key, value);
                const buffer = original.toBuffer();
                const deserialized = PartitionSegmentPayloadItem.from(buffer);

                expect(deserialized.getKey()).toBe(key);
                expect(deserialized.getValue()).toBe(value);
            });
        });
    });
});
