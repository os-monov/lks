import { Body, Controller, Get, Param, Post, Res } from '@nestjs/common';
import { IsNumber, IsString, Max, Min } from 'class-validator';
import { ApiResponse } from '@nestjs/swagger';
import { Response } from 'express';
import { Type } from 'class-transformer';
import { RecordLogManager } from './record/record.log.manager';
import {
  InternalServerException,
  InvalidRecordException,
  PartitionNotFoundException,
} from './exceptions';
import { Record } from './record/record';
import { FilePosition, Offset } from './record/types';
import { RecordCache } from './record/record.cache';
import { RecordLog } from './record/record.log';
import { ControlPlaneService } from './control.plane.service';

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
  value: string
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
  constructor(
    private readonly manager: RecordLogManager,
    private readonly cache: RecordCache,
    private readonly controlPlaneService: ControlPlaneService,
  ) { }

  @Post('produce/:partitionId')
  @ApiResponse({
    status: 200,
  })
  async produceRecord(
    @Param() params: ProduceRecordParams,
    @Body() input: ProduceRecordInput,
    @Res() response: Response,
  ): Promise<any> {
    const log = this.manager.getLog(params.partitionId);
    if (!log) {
      throw new PartitionNotFoundException();
    }

    const callback = (result: number | Error) => {
      if (result instanceof Error) {
        throw new InternalServerException();
      }
      response.json({ offset: result });
    };

    log.write(params.partitionId, input.key, input.value, callback);
  }

  @Get('fetch/:partitionId')
  @ApiResponse({
    status: 200,
    type: [Record],
  })
  async fetchRecords(
    @Param() params: FetchRecordsParams,
    @Res() response: Response,
  ): Promise<void> {
    const log: RecordLog = this.manager.getLog(params.partitionId);
    if (!log) {
      throw new PartitionNotFoundException();
    }

    const offset: Offset = this.cache.offset(params.partitionId);
    const position: FilePosition = this.controlPlaneService.findPosition(
      params.partitionId,
      offset,
    );
    const tail: Record[] = await log.query(params.partitionId, position);

    this.cache.insert(params.partitionId, tail);

    const result: Record[] = this.cache.get(params.partitionId);

    response.json(result);
  }
}
