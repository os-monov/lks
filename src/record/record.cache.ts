import { Injectable } from '@nestjs/common';
import { Record } from './record';
import { Offset, PartitionId } from '../segment/types';
const SortedSet = require('tlhunter-sorted-set');

/**
 * In-memory, per-partition record cache.
 */
@Injectable()
export class RecordCache {
  /**
   * Because "tlhunter-sorted-set" only supports unique primitives and not unique objects,
   * we need to maintain the set of unique offsets and then query for the appropriate
   * records using a side-car hash map.
   */
  private readonly cache: Map<
    PartitionId,
    {
      offsets: InstanceType<typeof SortedSet>;
      records: Map<number, Record>;
    }
  >;

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
      this.cache.set(partitionId, {
        offsets: new SortedSet(),
        records: new Map(),
      });
    }
    const { offsets, records: mapped } = this.cache.get(partitionId);
    for (const record of records) {
      const offset = Number(record.getOffset());
      offsets.add(offset, offset); // de-duplicate offsets
      mapped.set(offset, record); // last write wins
    }
  }

  /**
   * Retrieve all cached records for a given partition, in offset order.
   * @param partitionId Partition number.
   * @returns Array of Records (empty if partition not present).
   */
  public get(partitionId: PartitionId): Record[] {
    const entry = this.cache.get(partitionId);
    if (!entry) return [];
    const { offsets, records } = entry;
    return offsets.range(0, -1).map((offset: number) => records.get(offset));
  }

  /**
   * Get the offset of the latest record for a partition.
   * @param partitionId Partition number.
   * @returns Offset (bigint) of last record, or 0n if empty.
   */
  public latestOffset(partitionId: PartitionId): Offset {
    const entry = this.cache.get(partitionId);
    if (!entry || entry.offsets.card() === 0) return 0n;
    const lastOffset = entry.offsets.range(-1, -1)[0];
    return BigInt(lastOffset);
  }
}
