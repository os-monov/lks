import { Offset } from './types';

/**
 * Defines a key-value record.
 */
export class Record {
  constructor(
    private readonly offset: Offset,
    private readonly key: string,
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
