import { Body, Controller, Get, Param, Post, Res } from '@nestjs/common';
import { IsNumber, Max, Min } from 'class-validator';
import { ApiResponse } from '@nestjs/swagger';
import { Response } from 'express';
import { Type } from 'class-transformer';

export class ProduceRecordParams {
  @IsNumber()
  @Min(0)
  @Max(100)
  @Type(() => Number)
  partitionId: number;
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
  constructor() { }

  @Post('produce/:partitionId')
  @ApiResponse({
    status: 200,
  })
  async produceRecord(
    @Param() params: ProduceRecordParams,
    @Body() input: any,
    @Res() response: Response,
  ): Promise<any> {
    // writer = (bucket)
    // let message = new Message(0, 0, '');

    response.json();
  }

  @Get('fetch/:partitionId')
  @ApiResponse({
    status: 200,
    // type: [],
  })
  async fetchRecords(
    @Param() params: FetchRecordsParams,
    @Res() response: Response,
  ): Promise<void> {

    response.json([]);
  }
}
