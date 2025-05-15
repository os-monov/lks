import { Injectable } from '@nestjs/common';
import { RecordLog } from './record.log';
import { hash } from 'crypto';

@Injectable()
export class RecordLogManager {
  private readonly map = Map<number, RecordLog>;

  constructor(private readonly logCount = 4) {
    // for index in logCount
    // log = RecordLog(index)
  }

  getLog(partitionId: number): RecordLog {
    // const hash = hash(partitionId) % self.map
    // return self.map.get(hash)
    return;
  }
}
