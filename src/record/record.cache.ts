import { Record } from './record';
import { Offset, PartitionId } from './types';
const SortedSet = require('tlhunter-sorted-set');

export class RecordCache {
  private readonly cache: Map<number, typeof SortedSet>;

  constructor() {
    this.cache = new Map();
  }

  public insert(partitionId: PartitionId, records: Record[]): void {
    return;
  }

  public get(partitionId: PartitionId): Record[] {
    return [];
  }

  public offset(partitionId: PartitionId): Offset {
    return;
  }
}
