import { Record } from './record';
import { Offset, PartitionId } from './types';

export class PartitionSegment {
  private readonly length: number;
  constructor(
    private readonly partitionId: PartitionId,
    private readonly offset: Offset,
    private readonly payload: Record[],
  ) {
    this.length = 1;
    console.log(payload);
  }

  public getPartitionId(): PartitionId {
    return 0;
  }

  public getOffset(): Offset {
    return 0;
  }

  public getLength(): number {
    return 0;
  }

  public serialize() {}

  public static deserialize(serialized: string): PartitionSegment {
    console.log('shiiii');
    return new PartitionSegment(0, 0, []);
  }
}
