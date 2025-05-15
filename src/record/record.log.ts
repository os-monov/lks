import * as fs from 'fs';
import { Record } from './record';
import { ControlPlaneService } from 'src/control.plane.service';
import { Injectable } from '@nestjs/common';
import { FilePosition, PartitionId } from './types';

@Injectable()
export class RecordLog {
  private readonly dataDir = `/tmp/lks`;

  constructor(controlPlaneService: ControlPlaneService, index: number) {
    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir, { recursive: true });
    }

    // create /tmp/lks/index.log
  }

  public async write(
    partitionId: PartitionId,
    key: string,
    value: string,
    callback: Function,
  ): Promise<void> {
    // console.log(record)
    // create record
    console.log(callback);
  }

  private async flush() {
    // organize records by partitions
    // timestamp = now()
    // find records where timestamp <= now()
    // commits = []
    // for partition in partitions:
    //   commit = new PartitionCommit()
    // # ensure this is a transaction
    // await controlPlaneService.save(commits)
    // # resolve all callbacks
    // for record in records:
    //      record.callback.resolve()
  }

  public async query(
    partitionId: PartitionId,
    position: FilePosition,
  ): Promise<Record[]> {
    return [];
  }
}
