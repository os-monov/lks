import {
  Inject,
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import * as fs from 'fs';
import * as path from 'path';
import {
  FilePosition,
  Offset,
  PartitionCommit,
  PartitionId,
} from './segment/types';

@Injectable()
export class ControlPlaneService implements OnModuleInit, OnModuleDestroy {
  private readonly commits: Map<PartitionId, PartitionCommit[]> = new Map();
  private readonly dataDir = `/tmp/lks`;
  private readonly metadataFilePath: string = path.join(
    this.dataDir,
    'metadata.json',
  );

  constructor(
    @Inject('PARTITION_COUNT') private readonly partitionCount: number,
  ) {
    for (
      let partitionId = 0;
      partitionId < this.partitionCount;
      partitionId++
    ) {
      this.commits.set(partitionId, []);
    }
  }

  /**
   * Load metadata from file.
   */
  onModuleInit() {
    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir, { recursive: true });
    }

    if (fs.existsSync(this.metadataFilePath)) {
      try {
        const data = JSON.parse(fs.readFileSync(this.metadataFilePath, 'utf8'));
        this.commits.clear();

        for (const [partitionIdStr, commits] of Object.entries(data)) {
          this.commits.set(
            parseInt(partitionIdStr),
            commits as PartitionCommit[],
          );
        }
      } catch (error) {
        console.log('Failed to load control plane metadata as a file.');
        console.log(error);
      }
    }
  }

  /**
   * Save before shutting down.
   */
  onModuleDestroy() {
    this.save();
  }

  /**
   * Save metadata as a file.
   */
  private save(): void {
    try {
      const data = Object.fromEntries(this.commits);
      fs.writeFileSync(
        this.metadataFilePath,
        JSON.stringify(
          data,
          (_key, value) =>
            typeof value === 'bigint' ? value.toString() : value,
          2,
        ),
      );
    } catch (error) {
      console.log('Failed to save control plane metadata as a file.');
      console.log(error);
    }
  }

  /**
   * Returns the latest commit for a partition, or a default commit if none exists.
   * @param partitionId The partition to get the latest commit for
   * @returns The latest PartitionCommit
   */
  public latestCommit(partitionId: PartitionId): PartitionCommit {
    const commits: PartitionCommit[] = this.commits.get(partitionId);
    if (!commits || commits.length === 0) {
      return { partitionId: partitionId, offset: 1n, position: 0 };
    }
    return commits.at(commits.length - 1);
  }

  /**
   * Save new commits to the store and optionally persist to disk.
   * @param commits The commits to save
   * @param persist Whether to persist changes to disk (default: false)
   */
  public async saveCommits(commits: PartitionCommit[], persist: boolean = false): Promise<void> {
    // Validate commits (you could add more validation logic here)
    for (const commit of commits) {
      const existingCommits = this.commits.get(commit.partitionId);
      if (!existingCommits) {
        throw new Error(`Invalid partition ID: ${commit.partitionId}`);
      }

      // Optional: Add validation that new commits have higher offsets than previous ones
      if (existingCommits.length > 0) {
        const latestOffset = existingCommits[existingCommits.length - 1].offset;
        if (commit.offset <= latestOffset) {
          throw new Error(`New commit offset (${commit.offset}) must be greater than latest offset (${latestOffset})`);
        }
      }

      existingCommits.push(commit);
    }

    if (persist) {
      this.save();
    }
  }

  /**
   * Find the position of the commit with the highest offset lower than the provided offset.
   * @param partitionId
   * @param offset
   * @returns
   */
  public findPosition(partitionId: PartitionId, offset: Offset): FilePosition {
    const commits = this.commits.get(partitionId);
    if (!commits || commits.length === 0) {
      return 0;
    }

    // If offset is lower than the first commit, return 0
    if (offset < commits[0].offset) {
      return 0;
    }

    let lo = 0;
    let hi = commits.length;

    while (lo < hi) {
      const mid = Math.floor((lo + hi) / 2);
      const commit = commits[mid];
      if (commit.offset <= offset) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }

    // After binary search:
    // lo is the index of the smallest element greater than offset
    // lo-1 is the index of the largest element less than or equal to offset

    // If lo is out of bounds, it means all elements are <= offset
    if (lo >= commits.length) {
      return commits[commits.length - 1].position;
    }

    // If the algorithm stopped at the first element and it's greater than offset
    if (lo === 0) {
      return 0;
    }

    // Return the position of the last commit with offset <= target offset
    return commits[lo - 1].position;
  }
}
