import { Body, Controller, Get, Param, Post, Res } from '@nestjs/common';
import { IsNumber, IsString, Max, Min } from 'class-validator';
import { ApiResponse } from '@nestjs/swagger';
import { Response } from 'express';
import { Type } from 'class-transformer';
import { Message } from './types/message';
import { QueryService } from './query.service';
import { BatchedMessageWriter } from './batched.message.writer';

export class ProduceMessageParams {
  @IsString()
  topic: string;

  @IsNumber()
  @Min(0)
  @Max(100)
  @Type(() => Number)
  partitionId: number;
}

export class FetchMessagesParams {
  @IsString()
  topic: string;

  @IsNumber()
  @Min(0)
  @Max(100)
  @Type(() => Number)
  partitionId: number;
}

@Controller()
export class DataPlaneController {
  constructor(
    private readonly writer: BatchedMessageWriter,
    private readonly queryService: QueryService,
  ) {}

  @Post('topics/:topic/produce/:partitionId')
  @ApiResponse({
    status: 200,
    type: Message,
  })
  async produceMessage(
    @Param() params: ProduceMessageParams,
    @Body() input: any,
    @Res() response: Response,
  ): Promise<any> {
    // writer = BatchWriter(bucket)
    let message = new Message(0, 0, '');
    // const buffered: BufferedMessage = await this.writer.write(message)

    response.json(message);
  }

  @Get('topics/:topic/fetch/:partition?offset=&count=')
  @ApiResponse({
    status: 200,
    type: [Message],
  })
  async fetchMessages(
    @Param() params: FetchMessagesParams,
    @Res() response: Response,
  ): Promise<void> {
    // messages = await this.queryService.query(
    //   params.topic,
    //   params.partitionId,
    //   params.offset ? params.offset : 0
    // )

    response.json([]);
  }
}
