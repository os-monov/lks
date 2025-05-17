import * as fs from 'fs';
import { PartitionSegmentHeader } from '../segment/partition.segment.header';
import { PartitionSegment } from '../segment/partition.segment';
import * as path from 'path';

export class RecordLogReader {
    constructor(
        private readonly logFilePath: string
    ) {

    }


    public scan() {
        if (!fs.existsSync(this.logFilePath)) {
            console.log(`Log file ${this.logFilePath} not found`);
            return;
        }

        const fileBuf = fs.readFileSync(this.logFilePath);
        const outLines: string[] = [];

        let position = 0;
        while (position < fileBuf.length) {
            /* 1. Guard: make sure header is complete */
            if (position + PartitionSegmentHeader.SIZE > fileBuf.length) {
                console.log('Incomplete header at end of file - aborting scan');
                break;
            }

            /* 2. Parse header to know payload length */
            const headerBuf = fileBuf.subarray(
                position,
                position + PartitionSegmentHeader.SIZE,
            );
            const header = PartitionSegmentHeader.from(headerBuf);
            const payloadEnd =
                position + PartitionSegmentHeader.SIZE + header.getPayloadLength();

            /* 3. Guard: make sure payload is complete */
            if (payloadEnd > fileBuf.length) {
                console.log(
                    'Incomplete payload for final segment - aborting scan',
                );
                break;
            }

            /* 4. Slice full segment buffer and reconstruct segment */
            const segmentBuf = fileBuf.subarray(position, payloadEnd);
            const segment = PartitionSegment.from(segmentBuf);

            /* Output segment information in requested format */
            outLines.push(`partition #${segment.getPartitionId()}`);
            segment.getRecords().forEach((rec) => {
                outLines.push(`${rec.getOffset()} | ${rec.getKey()} | ${rec.getValue()}`);
            });
            outLines.push(''); // Empty line between segments

            /* Advance to the next segment */
            position = payloadEnd;
        }

        const outPath = path.resolve('/tmp/lks', 'segments.log');
        fs.writeFileSync(outPath, outLines.join('\n'));
        console.log(`Segments log written to ${outPath}`);
    }



    //     /**
    //  * Query the log file for the relevant records.
    //  * @param partitionId
    //  * @param position
    //  * @returns
    //  */
    //     public async query(
    //         partitionId: PartitionId,
    //         position: FilePosition,
    //     ): Promise<Record[]> {
    //         if (this.position === 0) {
    //             return [];
    //         }

    //         // segmentHeader =

    //         // fs.createReadStream(this.logFile, { start,  end })
    //         // const data =
    //     }

    // async onDestroy() {
    //     clearInterval(this.flushInterval);
    //     await this.flush();
    //     console.log(`[RecordLog ${this.index}] Flushed on destroy`);
    // }
}