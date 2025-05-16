import { Record } from '../record/record';
import { Offset, PartitionId } from './types';

export class PartitionSegment {
  private readonly length: number;

  constructor(
    private readonly partitionId: PartitionId,
    private readonly offset: Offset,
    private readonly payload: Record[],
  ) {
    // Calculate length during construction
  }

  public getPartitionId(): PartitionId {
    return this.partitionId;
  }

  public getOffset(): Offset {
    return this.offset;
  }

  public getLength(): number {
    return this.length;
  }

  public getPayload(): Record[] {
    return this.payload;
  }
}
