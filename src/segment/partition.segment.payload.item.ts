import { Buffer } from 'buffer';
import { InvalidRecordException } from '../exceptions';

export class PartitionSegmentPayloadItem {
    private static readonly MAX_KEY_SIZE = 1024; // 1KB
    private static readonly MAX_VALUE_SIZE = 1024; // 1KB
    private static readonly LENGTH_SIZE = 4; // 4 Bytes

    /**
     * Immutable constructor.
     * @param key
     * @param value
     */
    constructor(
        private readonly key: string,
        private readonly value: string,
    ) {
        if (
            Buffer.byteLength(key, 'utf8') > PartitionSegmentPayloadItem.MAX_KEY_SIZE
        ) {
            throw new InvalidRecordException();
        }

        if (
            Buffer.byteLength(value, 'utf8') >
            PartitionSegmentPayloadItem.MAX_VALUE_SIZE
        ) {
            throw new InvalidRecordException();
        }
    }

    /**
     * Creates a new instance from a buffer that contains exactly one payload item.
     * @param buffer
     * @returns
     */
    static from(buffer: Buffer): PartitionSegmentPayloadItem {
        const keyLength = buffer.readUint32BE(0);
        const key = buffer.toString(
            'utf8',
            PartitionSegmentPayloadItem.LENGTH_SIZE,
            PartitionSegmentPayloadItem.LENGTH_SIZE + keyLength,
        );

        const valueLength = buffer.readUint32BE(4 + keyLength);
        const value = buffer.toString(
            'utf8',
            PartitionSegmentPayloadItem.LENGTH_SIZE * 2 + keyLength,
            PartitionSegmentPayloadItem.LENGTH_SIZE * 2 + keyLength + valueLength,
        );

        return new PartitionSegmentPayloadItem(key, value);
    }

    /**
     * Create buffer from instance.
     */
    public toBuffer(): Buffer {
        const keyBuffer = Buffer.from(this.key, 'utf8');
        const valueBuffer = Buffer.from(this.value, 'utf8');

        const buffer = Buffer.alloc(
            2 * PartitionSegmentPayloadItem.LENGTH_SIZE +
            keyBuffer.length +
            valueBuffer.length,
        );

        let offset = 0;

        buffer.writeUInt32BE(keyBuffer.length, offset);
        offset += PartitionSegmentPayloadItem.LENGTH_SIZE;

        keyBuffer.copy(buffer, offset);
        offset += keyBuffer.length;

        buffer.writeUint32BE(valueBuffer.length, offset);
        offset += PartitionSegmentPayloadItem.LENGTH_SIZE;

        valueBuffer.copy(buffer, offset);
        offset += valueBuffer.length;

        return buffer;
    }

    /**
     * Calculate size in bytes when serialized
     * @returns
     */
    getSize(): number {
        const keyBytes = Buffer.byteLength(this.key, 'utf8');
        const valueBytes = Buffer.byteLength(this.value, 'utf8');
        return 2 * PartitionSegmentPayloadItem.LENGTH_SIZE + keyBytes + valueBytes;
    }

    /**
     * Get the key
     */
    getKey(): string {
        return this.key;
    }

    /**
     * Get the value
     */
    getValue(): string {
        return this.value;
    }
}
