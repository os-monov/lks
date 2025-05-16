import { Injectable } from '@nestjs/common';
import { Record } from './record';
import { Offset, PartitionId } from './types';
const SortedSet = require('tlhunter-sorted-set');

@Injectable()
export class RecordCache {
  private readonly cache: Map<number, typeof SortedSet>;

  constructor() {
    this.cache = new Map();
  }

  /**
   * Insert records into the cache for the provided partition.
   * @param partitionId
   * @param records
   */
  public insert(partitionId: PartitionId, records: Record[]): void {
    const existing = this.cache.get(partitionId);
    for (const record of records) {
      existing.add(record, record.getOffset());
    }
  }

  /**
   * Retrieve records for the provided partition.
   * @param partitionId
   * @returns
   */
  public get(partitionId: PartitionId): Record[] {
    const partitionSet = this.cache.get(partitionId);
    if (!partitionSet) {
      return [];
    }

    // Get all elements (0 to -1 gets entire range, similar to Redis)
    return partitionSet.range(0, -1);
  }

  /**
   * Returns the offset of the last record.
   * @param partitionId
   * @returns
   */
  public offset(partitionId: PartitionId): Offset {
    const records = this.cache.get(partitionId);
    if (!records || records.card() === 0) {
      return 0;
    }

    // Get the last element (highest score/offset)
    // range(-1) gets the last element in the set
    const last = records.range(-1, -1)[0];

    // Return the offset of the last record
    return last.offset;
  }
}
