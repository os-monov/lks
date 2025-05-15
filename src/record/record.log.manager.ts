import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { RecordLog } from './record.log';
import { ControlPlaneService } from 'src/control.plane.service';
import { Offset, PartitionId } from './types';



@Injectable()
export class RecordLogManager implements OnModuleInit {
    private logs: RecordLog[] = [];

    constructor(
        private readonly controlPlaneService: ControlPlaneService,
        @Inject("PARTITION_COUNT") private readonly partitionCount: number,
        @Inject('LOG_COUNT') private readonly logCount: number,
    ) { }

    async onModuleInit() {
        for (let logIndex = 0; logIndex < this.logCount; logIndex++) {
            const cursors: { partitionId: PartitionId, offset: Offset }[] = [];

            // For each possible partition ID
            for (let partitionId = 0; partitionId < this.partitionCount; partitionId++) {
                // Check if this partition belongs to this log file
                if (partitionId % this.logCount === logIndex) {
                    // Get the last committed offset for this partition
                    const offset = this.controlPlaneService.offset(partitionId);
                    cursors.push({ partitionId, offset });
                }
            }

            // Create RecordLog with its partitions and their offsets
            console.log(`Initializing RecordLog ${logIndex} with ${cursors.length} partitions.`)
            console.log(`Partitions: ${cursors.map(c => `${c.partitionId}:${c.offset}`).join(', ')}`);
            this.logs.push(new RecordLog(this.controlPlaneService, logIndex, cursors));
            console.log(`RecordLogManager: Initialized RecordLog ${logIndex} with ${cursors.length} partitions`);
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
