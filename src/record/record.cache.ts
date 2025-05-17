import { Injectable } from '@nestjs/common';
import { Record } from './record';
import { Offset, PartitionId } from '../segment/types';
const SortedSet = require('tlhunter-sorted-set');

/**
 * In-memory, per-partition record cache with fast ordered access.
 */
@Injectable()
export class RecordCache {
  /**
   * Map from partition ID to SortedSet of records.
   * Sorted by offset (score).
   */
  private readonly cache: Map<PartitionId, InstanceType<typeof SortedSet>>;

  constructor() {
    this.cache = new Map();
  }

  /**
   * Insert records into the cache for the provided partition.
   * Will create a SortedSet for the partition if it does not exist.
   * @param partitionId Partition number.
   * @param records Array of Record objects to insert.
   */
  public insert(partitionId: PartitionId, records: Record[]): void {
    if (!this.cache.has(partitionId)) {
      this.cache.set(partitionId, new SortedSet());
    }
    const set = this.cache.get(partitionId);
    for (const record of records) {
      set.add(record, Number(record.getOffset())); // Score must be a number, not bigint!
    }
  }

  /**
   * Retrieve all cached records for a given partition, in offset order.
   * @param partitionId Partition number.
   * @returns Array of Records (empty if partition not present).
   */
  public get(partitionId: PartitionId): Record[] {
    const set = this.cache.get(partitionId);
    if (!set) return [];
    return set.range(0, -1); // All records
  }

  /**
   * Get the offset of the last (highest) record for a partition.
   * @param partitionId Partition number.
   * @returns Offset (bigint) of last record, or 0n if empty.
   */
  public offset(partitionId: PartitionId): Offset {
    const set = this.cache.get(partitionId);
    if (!set || set.card() === 0) return 0n;
    const last: Record = set.range(-1, -1)[0];
    return last.getOffset();
  }

  /**
   * Return the number of cached records for a partition.
   * @param partitionId Partition number.
   * @returns Record count.
   */
  public count(partitionId: PartitionId): number {
    const set = this.cache.get(partitionId);
    return set ? set.card() : 0;
  }

  /**
   * Clear the cache for a given partition (for memory control/testing).
   * @param partitionId Partition number.
   */
  public clear(partitionId: PartitionId): void {
    this.cache.delete(partitionId);
  }

  /**
   * Remove all data from the cache.
   */
  public clearAll(): void {
    this.cache.clear();
  }
}
