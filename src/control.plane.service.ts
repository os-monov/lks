import { OnModuleInit } from '@nestjs/common';
import * as fs from 'fs';
import * as path from 'path';
import {
  FilePosition,
  Offset,
  PartitionCommit,
  PartitionId,
} from './segment/types';
import { InternalServerException } from './exceptions';

export class ControlPlaneService implements OnModuleInit {
  private readonly commits: Map<PartitionId, PartitionCommit[]> = new Map();

  constructor(
    private readonly metadataFilePath: string,
    private readonly partitionCount: number,
  ) {}

  /**
   * Load metadata from file.
   */
  onModuleInit() {
    const dir = path.dirname(this.metadataFilePath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    this.commits.clear();
    for (
      let partitionId = 0;
      partitionId < this.partitionCount;
      partitionId++
    ) {
      this.commits.set(partitionId, []);
    }

    if (fs.existsSync(this.metadataFilePath)) {
      const lines = fs
        .readFileSync(this.metadataFilePath, 'utf8')
        .split('\n')
        .filter(Boolean);
      for (const line of lines) {
        try {
          // All offsets persisted as strings, restore to BigInt
          const parsed = JSON.parse(line) as any;
          const commit: PartitionCommit = {
            partitionId: parsed.partitionId,
            position: parsed.position,
            offset: BigInt(parsed.offset),
          };

          const curr = this.commits.get(commit.partitionId);
          if (!curr) throw new InternalServerException();
          curr.push(commit);
        } catch (err) {
          console.log('Corrupt commit line in metadata:', line, err);
        }
      }
    }
  }

  /**
   * Synchronously save each commit as a JSON line.
   */
  private saveToFile(commits: PartitionCommit[]): void {
    // Convert BigInt offset to string for JSON
    const lines = commits
      .map(
        (commit) =>
          JSON.stringify({
            ...commit,
            offset: commit.offset.toString(),
          }) + '\n',
      )
      .join('');

    // Append synchronously for durability
    fs.appendFileSync(this.metadataFilePath, lines, { encoding: 'utf8' });
  }

  /**
   * Save new commits in-memory and then to file.
   * @param commits
   */
  public async saveCommits(commits: PartitionCommit[]): Promise<void> {
    for (const commit of commits) {
      const existingCommits = this.commits.get(commit.partitionId);
      if (!existingCommits) {
        throw new InternalServerException();
      }
      if (existingCommits.length > 0) {
        const latestOffset = existingCommits[existingCommits.length - 1].offset;
        if (commit.offset <= latestOffset) {
          throw new InternalServerException(
            `New commit offset (${commit.offset}) must be greater than latest offset (${latestOffset})`,
          );
        }
      }
      existingCommits.push(commit);
    }
    this.saveToFile(commits);
  }

  /**
   * Returns the latest commit for a partition, or a default commit if none exists.
   * @param partitionId The partition to get the latest commit for
   * @returns The latest PartitionCommit
   */
  public latestCommit(partitionId: PartitionId): PartitionCommit {
    const commits = this.commits.get(partitionId) ?? [];
    if (commits.length === 0) {
      return { partitionId, offset: 1n, position: 0 };
    }
    return commits[commits.length - 1];
  }

  /**
   * Find the position of the commit with the highest offset lower than the provided offset.
   * @param partitionId
   * @param offset
   * @returns
   */
  public findPosition(partitionId: PartitionId, offset: Offset): FilePosition {
    const commits = this.commits.get(partitionId);
    if (!commits) return 0;
    if (!commits.length) return 0;

    if (offset < commits[0].offset) return 0;

    // Binary search: find the largest commit.offset <= target offset
    let lo = 0,
      hi = commits.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (commits[mid].offset <= offset) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }

    // lo now is the index of the first commit with offset > target
    // so lo - 1 is the last commit <= target
    return lo === 0 ? 0 : commits[lo - 1].position;
  }
}
