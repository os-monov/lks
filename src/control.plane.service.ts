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

  constructor() {
    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir, { recursive: true });
    }
  }

  public async commit(commits: PartitionCommit[]): Promise<void> {
    // ensure all commits are valid or throw exception
    // validate that all commits are the expected ones and we don't have an older offset beating an earlier one
    // write to disk to persist (db)
  }

  public findPosition(partitionId: PartitionId, offset: Offset): FilePosition {
    // console.log(test)
    return 0;
  }
}
