import {
  Body,
  Controller,
  Get,
  Header,
  Inject,
  Param,
  Post,
  Res,
} from '@nestjs/common';
import { IsNumber, IsString, Max, Min } from 'class-validator';
import { ApiResponse } from '@nestjs/swagger';
import { Response } from 'express';
import { Type } from 'class-transformer';
import { PartitionNotFoundException } from './exceptions';
import { FilePosition, Offset, PartitionId } from './segment/types';
import { RecordCache } from './record/record.cache';
import { ControlPlaneService } from './control.plane.service';
import { RecordLogWriter } from './record/record.log.writer';
import { RecordLogReader } from './record/record.log.reader';
import * as path from 'path';
import * as fs from 'fs';
import { Record } from './record/record';
import { MetricsService } from './metrics.service';
import { ConsoleLogger } from './console.logger';

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
  private readonly controlPlaneService: ControlPlaneService;
  private readonly writers: RecordLogWriter[];
  private readonly readers: RecordLogReader[];

  constructor(
    @Inject('PARTITION_COUNT') partitionCount: number,
    @Inject('LOG_COUNT') logCount: number,
    private readonly cache: RecordCache,
    private readonly metricsService: MetricsService,
    private readonly logger: ConsoleLogger,
  ) {
    if (!fs.existsSync(this.baseDirectory)) {
      fs.mkdirSync(this.baseDirectory, { recursive: true });
    }

    const metadataFilePath = path.join(this.baseDirectory, `metadata.ndjson`);
    this.controlPlaneService = new ControlPlaneService(
      metadataFilePath,
      partitionCount,
      logger,
    );
    this.readers = this.initializeReaders(logCount);
    this.writers = this.initializeWriters(logCount, partitionCount);
  }

  @Post('produce/:partitionId')
  @Header('Content-Type', 'application/x-www-form-urlencoded')
  @ApiResponse({
    status: 200,
  })
  async produceRecord(
    @Param() params: ProduceRecordParams,
    @Body() input: any,
    @Res() response: Response,
  ): Promise<any> {
    const start = Date.now();
    const writer = this.getWriter(params.partitionId);
    if (!writer) {
      this.logger.error(
        `No record log writer found for partition: ${params.partitionId}`,
      );
      throw new PartitionNotFoundException();
    }

    const offset: Offset = await writer.write(
      params.partitionId,
      input.key,
      input.value,
    );
    this.metricsService.emit('api.produce', Date.now() - start);

    response.json({ offset: offset.toString() });
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
    const start = Date.now();

    this.logger.info(
      `[${params.partitionId}]: Querying records for partition.`,
    );
    const reader: RecordLogReader = this.getReader(params.partitionId);
    if (!reader) {
      this.logger.info(`[${params.partitionId}: Partition not found.`);
      throw new PartitionNotFoundException();
    }

    const offset: Offset = this.cache.latestOffset(params.partitionId);
    this.logger.info(
      `[${params.partitionId}]: Querying records for partition.`,
    );
    this.logger.info(
      `[${params.partitionId}]: Last queried offset is: ${offset}`,
    );
    const position: FilePosition = this.controlPlaneService.findPosition(
      params.partitionId,
      offset,
    );
    const tail: Record[] = await reader.query(params.partitionId, position);
    this.logger.info(
      `[${params.partitionId}]: Found ${tail.length} new records.`,
    );

    this.cache.insert(params.partitionId, tail);

    // convert big int to string
    const result: any[] = this.cache.get(params.partitionId).map((r) => ({
      offset: r.getOffset().toString(),
      key: r.getKey(),
      value: r.getValue(),
    }));
    this.logger.info(
      `[${params.partitionId}]: Returning ${result.length} total records.`,
    );

    this.metricsService.emit('api.fetch', Date.now() - start);
    this.metricsService.emit('fetch.count', result.length);
    response.json(result);
  }

  /**
   * Initialize readers.
   * @param count
   * @returns
   */
  private initializeReaders(shards: number): RecordLogReader[] {
    return Array.from({ length: shards }, (_, i) => {
      const logFilePath = path.join(this.baseDirectory, `${i}.log`);
      return new RecordLogReader(logFilePath, this.metricsService, this.logger);
    });
  }

  /**
   * Initialize writers.
   * @param count
   * @param partitions
   * @returns
   */
  private initializeWriters(
    shards: number,
    partitions: number,
  ): RecordLogWriter[] {
    return Array.from({ length: shards }, (_, i) => {
      const logFilePath = path.join(this.baseDirectory, `${i}.log`);

      const partitionIds: PartitionId[] = Array.from(
        { length: partitions },
        (_, idx) => idx,
      ).filter((p) => p % shards === i);

      const offsets: Map<PartitionId, Offset> = new Map();
      for (const partitionId of partitionIds) {
        // queries the control plane service for the latest partition commit
        const latestCommit = this.controlPlaneService.latestCommit(partitionId);
        // increment offset by 1 from last COMMITTED offset
        const startOffset = latestCommit.offset + 1n;
        offsets.set(partitionId, startOffset);
        this.logger.info(
          `[Writer @ ${logFilePath}] Partition ${partitionId} is currently at offset: ${startOffset}.`,
        );
      }
      return new RecordLogWriter(
        logFilePath,
        0,
        offsets,
        this.controlPlaneService,
        this.metricsService,
        this.logger,
      );
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
}
