import { Injectable } from '@nestjs/common';
import * as fs from 'fs';
import * as path from 'path';
import { plainToInstance } from 'class-transformer';
import {
  FilePosition,
  Offset,
  PartitionCommit,
  PartitionId,
} from './record/types';

@Injectable()
export class ControlPlaneService {
  private readonly commits: Map<PartitionId, PartitionCommit[]> = new Map();
  private readonly dataDir = `/tmp/lks`;
  private readonly metadataFilePath: string = path.join(this.dataDir, "metadata.json");

  constructor() {
    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir, { recursive: true });
    }

    this.load();
  }

  /**
   * Load metadata from a file.
   */
  private async load(): Promise<void> {
    if (fs.existsSync(this.metadataFilePath)) {
      try {
        // lets store metadata.json as { partition: PartitionCommit[] }


      } catch (error) {
        console.log("Failed to load control plane metadata as a file.")
        console.log(error);
      }
    }
  }

  /**
   * Save metadata as a file.
   */
  private save(): void {
    try {
      fs.writeFileSync(this.metadataFilePath, JSON.stringify(JSON.stringify(this.commits), null, 2))

    } catch (error) {
      console.log("Failed to save control plane metadata as a file.")
      console.log(error);
    }
  }

  /**
   * Returns the last committed offset for a partition.
   * @param partitionId 
   * @returns 
   */
  public offset(partitionId: PartitionId): Offset {
    return 1
  }

  public async commit(commits: PartitionCommit[]): Promise<void> {
    // ensure all commits are valid or throw exception
    // validate that all commits are the expected ones and we don't have an older offset beating an earlier one
    // write to disk to persist (db)

    this.save();
  }

  public findPosition(partitionId: PartitionId, offset: Offset): FilePosition {
    // console.log(test)
    return 0;
  }
}
