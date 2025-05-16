import { Offset } from '../segment/types';

/**
 * Defines a record.
 */
export class Record {
  constructor(
    /**
     * Offset of the record within the partition, e.g. 1003.
     */
    private readonly offset: Offset,
    /**
     * Key for the record, e.g. "cat".
     */
    private readonly key: string,
    /**
     * Value for the record, e.g. "dog".
     */
    private readonly value: string,
  ) {}

  /**
   * Get offset.
   * @returns
   */
  getOffset(): Offset {
    return this.offset;
  }

  /**
   * Gets key.
   * @returns
   */
  getKey(): string {
    return this.key;
  }

  /**
   * Gets value.
   * @returns
   */
  getValue(): string {
    return this.value;
  }
}
