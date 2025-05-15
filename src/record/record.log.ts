import * as fs from 'fs';
import { Record } from './record';
import { ControlPlaneService } from 'src/control.plane.service';
import { FilePosition, PartitionId } from './types';
import * as path from 'path';
import { BufferRecord } from './buffer.record';

export class RecordLog {
  private readonly dataDir: string;
  private readonly logFile: string;
  private readonly buffer: BufferRecord[] = [];

  constructor(
    private readonly controlPlaneService: ControlPlaneService,
    readonly index: number,
  ) {
    this.dataDir = `/tmp/lks`;
    this.logFile = path.join(this.dataDir, `${index}.log`);

    // Ensure data directory exists
    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir, { recursive: true });
    }

    // Create log file if it doesn't exist
    if (!fs.existsSync(this.logFile)) {
      fs.writeFileSync(this.logFile, '');
      console.log(`Created log file: ${this.logFile}`);
    }
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
