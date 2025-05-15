import { Inject, Injectable } from '@nestjs/common';
import { RecordLog } from './record.log';
import { ControlPlaneService } from 'src/control.plane.service';

@Injectable()
export class RecordLogManager {
  private logs: RecordLog[] = [];

  constructor(
    private readonly controlPlaneService: ControlPlaneService,
    @Inject('LOG_COUNT') private readonly logCount: number,
  ) {
    for (let index = 0; index < this.logCount; index++) {
      this.logs.push(new RecordLog(this.controlPlaneService, index));
    }
  }

  /**
   * Gets {@link RecordLog} that partition should write to.
   * @param partitionId
   * @returns
   */
  getLog(partitionId: number): RecordLog {
    const index = partitionId % this.logs.length;
    return this.logs[index];
  }
}
