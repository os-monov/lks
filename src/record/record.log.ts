import * as fs from 'fs';
import { Record } from './record';
import { ControlPlaneService } from 'src/control.plane.service';
import { FilePosition, Offset, PartitionId } from './types';
import * as path from 'path';
import { BufferRecord } from './buffer.record';
import { InternalServerException } from 'src/exceptions';

export class RecordLog {
    private readonly dataDir: string;
    private readonly logFile: string;
    private readonly offsets: Map<PartitionId, Offset> = new Map();
    private readonly partitions: Set<PartitionId> = new Set();
    private readonly activeBuffers: Map<PartitionId, BufferRecord[]> = new Map();
    private readonly flushBuffers: Map<PartitionId, BufferRecord[]> = new Map();

    constructor(
        private readonly controlPlaneService: ControlPlaneService,
        readonly index: number,
        cursors: { partitionId: PartitionId, offset: Offset }[]
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

        // Initialize partitions and their offsets
        for (const cursor of cursors) {
            this.partitions.add(cursor.partitionId);
            this.offsets.set(cursor.partitionId, cursor.offset);
        }
    }

    /**
     * Creates a {@link BufferRecord}.
     * @param partitionId 
     * @param key 
     * @param value 
     * @param callback 
     */
    public async write(
        partitionId: PartitionId,
        key: string,
        value: string,
        callback: Function,
    ): Promise<void> {
        try {

            if (!this.partitions.has(partitionId)) {
                throw new InternalServerException();
            }


            const offset = this.offsets.get(partitionId);
            console.log(`(${partitionId}, ${offset} ${key}:${value}`)
            this.offsets.set(partitionId, offset + 1);

            callback(offset);
            // const bufferRecord = new BufferRecord(offset, key, value, new Date(), callback)

            // if (!this.activeBuffers.has(partitionId)) {
            //     this.activeBuffers.set(partitionId, [])
            // }

            // this.activeBuffers.get(partitionId).push(bufferRecord);
            // this.offsets[partitionId] += 1

        } catch (error) {
            console.error(`[RecordLog ${this.index}] Error in write (local offset) for partition ${partitionId}:`, error);
            callback(error instanceof Error ? error : new Error('Failed to prepare record for write with local offset'));
        }
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
