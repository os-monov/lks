import { Record } from './record';
import { Offset, PartitionId } from '../segment/types';

/**
 * Extends {@link Record} with a promise that is resolved
 * when the record is successfully persisted.
 */
export class BufferRecord extends Record {
  private readonly partitionId: PartitionId;
  public readonly promise: Promise<Offset>;
  private _resolve: (offset: Offset) => void;
  private _reject: (error: Error) => void;

  constructor(
    partitionId: PartitionId,
    offset: Offset,
    key: string,
    value: string,
  ) {
    super(offset, key, value);
    this.partitionId = partitionId;
    this.promise = new Promise<Offset>((resolve, reject) => {
      this._resolve = resolve;
      this._reject = reject;
    });
  }

  /**
   * Returns the partition id.
   * @returns
   */
  getPartitionId(): PartitionId {
    return this.partitionId;
  }

  /**
   * Resolve the record.
   * @param offset
   */
  resolve(offset: Offset): void {
    this._resolve(offset);
  }

  /**
   * Reject the record.
   * @param error
   */
  reject(error: Error): void {
    this._reject(error);
  }
}
