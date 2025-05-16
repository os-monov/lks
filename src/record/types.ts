/**
 * Explicitly type as numbers.
 */
export type PartitionId = number;
export type Offset = number;
export type FilePosition = number;

export interface PartitionCommit {
  partitionId: PartitionId;
  offset: Offset;
  position: FilePosition;
}
