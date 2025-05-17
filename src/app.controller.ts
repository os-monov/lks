import { Body, Controller, Inject, Param, Post, Res } from '@nestjs/common';
import { IsNumber, IsString, Max, Min } from 'class-validator';
import { ApiResponse } from '@nestjs/swagger';
import { Response } from 'express';
import { Type } from 'class-transformer';
import { PartitionNotFoundException } from './exceptions';
import { Offset, PartitionId } from './segment/types';
import { RecordCache } from './record/record.cache';
import { ControlPlaneService } from './control.plane.service';
import {
  RecordLogWriter,
  RecordLogWriterConfiguration,
} from './record/record.log.writer';
import { RecordLogReader } from './record/record.log.reader';
import * as path from 'path';
import * as fs from 'fs';

export class ProduceRecordParams {
  @IsNumber()
  @Min(0)
  @Max(100)
  @Type(() => Number)
  partitionId: number;
}

export class ProduceRecordInput {
  @IsString()
  key: string;

  @IsString()
  value: string;
}

export class FetchRecordsParams {
  @IsNumber()
  @Min(0)
  @Max(100)
  @Type(() => Number)
  partitionId: number;
}

@Controller()
export class AppController {
  private readonly baseDirectory: string = '/tmp/lks';
  private readonly writers: RecordLogWriter[];
  private readonly readers: RecordLogReader[];

  constructor(
    @Inject('PARTITION_COUNT') partitionCount: number,
    @Inject('LOG_COUNT') logCount: number,
    private readonly cache: RecordCache,
    private readonly controlPlaneService: ControlPlaneService,
  ) {
    if (!fs.existsSync(this.baseDirectory)) {
      fs.mkdirSync(this.baseDirectory, { recursive: true });
    }

    this.writers = Array.from({ length: logCount }, (_, i) => {
      const logFilePath = path.join(this.baseDirectory, `${i}.log`);

      const partitions: PartitionId[] = Array.from(
        { length: partitionCount },
        (_, idx) => idx, //  ← idx is a number
      ).filter((p) => p % logCount === i);

      const offsets: Map<PartitionId, Offset> = new Map();
      for (const partition of partitions) {
        const lastCommit = this.controlPlaneService.lastCommit(partition);
        offsets.set(partition, lastCommit.offset);
      }
      const config: RecordLogWriterConfiguration = {
        logFilePath: logFilePath,
        offsets: offsets,
        position: 0,
        onCommit: controlPlaneService.commit,
      };
      return new RecordLogWriter(config);
    });
  }

  /**
   * Returns reader for specific partition.
   * @param partitionId
   * @returns
   */
  private getReader(partitionId: PartitionId): RecordLogReader {
    return this.readers[partitionId % this.readers.length];
  }

  /**
   * Returns writer for specific partition.
   * @param partitionId
   * @returns
   */
  private getWriter(partitionId: PartitionId): RecordLogWriter {
    return this.writers[partitionId % this.writers.length];
  }

  @Post('produce/:partitionId')
  @ApiResponse({
    status: 200,
  })
  async produceRecord(
    @Param() params: ProduceRecordParams,
    @Body() input: ProduceRecordInput,
    @Res() response: Response,
  ): Promise<any> {
    const writer = this.getWriter(params.partitionId);
    if (!writer) {
      console.log(
        `No record log writer found for partition: ${params.partitionId}`,
      );
      throw new PartitionNotFoundException();
    }

    const offset: Offset = await writer.write(
      params.partitionId,
      input.key,
      input.value,
    );
    response.json({ offset: offset.toString() });
  }

  // @Get('fetch/:partitionId')
  // @ApiResponse({
  //   status: 200,
  //   type: [Record],
  // })
  // async fetchRecords(
  //   @Param() params: FetchRecordsParams,
  //   @Res() response: Response,
  // ): Promise<void> {
  //   const log: RecordLog = this.manager.getLog(params.partitionId);
  //   if (!log) {
  //     throw new PartitionNotFoundException();
  //   }

  //   const offset: Offset = this.cache.offset(params.partitionId);
  //   const position: FilePosition = this.controlPlaneService.findPosition(
  //     params.partitionId,
  //     offset,
  //   );
  //   const tail: Record[] = await log.query(params.partitionId, position);

  //   this.cache.insert(params.partitionId, tail);

  //   const result: Record[] = this.cache.get(params.partitionId);

  //   response.json(result);
  // }
}
