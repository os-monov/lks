import { Body, Controller, Param, Post, Res } from '@nestjs/common';
import { IsNumber, IsString, Max, Min } from 'class-validator';
import { ApiResponse } from '@nestjs/swagger';
import Topic from './types/topic';
import { MetadataService } from './metadata.service';
import { Response } from 'express';
import { Type } from 'class-transformer';

export class CreateTopicRequest {
  @IsString()
  name: string;
}

export class ProduceMessageParams {
  @IsString()
  topic: string;

  @IsNumber()
  @Min(0)
  @Max(100)
  @Type(() => Number)
  partitionId: number;
}

export class FetchMessagesRequest {}

export class Message {
  // message id
  // size
  // partition id
  // offset
  // data
}

export class MessageFile {}

@Controller()
export class AppController {
  constructor(private readonly metadataService: MetadataService) {}

  @Post('/topics')
  @ApiResponse({
    status: 200,
    type: Topic,
  })
  createTopic(
    @Body() input: CreateTopicRequest,
    @Res() response: Response,
  ): any {
    const topic = this.metadataService.createTopic(input.name);
    response.json(topic);
  }

  @Post('topics/:topic/produce/:partitionId')
  @ApiResponse({
    status: 200,
    type: Message,
  })
  produceMessage(
    @Param() params: ProduceMessageParams,
    @Body() input: any,
    @Res() response: Response,
  ) {
    console.log(params);
    console.log(input);

    // size limit 1KB
    // topic doesn't exist, throw invalild

    // writer = BatchWriter(bucket)
    // callback =  writer.write(message)
    // callback.on("success", () ->
    //     response.json({messageId:, offset: number})
    // )

    // callback.on('failure', () => {
    //})
    response.json();
  }

  // @Get("topics/:topic/fetch/:partition?offset=&count=")
  // @ApiOperation({
  //   status: 200,
  //   type: [Message]
  // })
  // fetchMessages() {

  //   // if
  //   // files: File[] = metadataService.getFiles(topic, partition, offset)
  //   // for file in files
  //   //  for message in file.messages()
  //   //      cache.addFile(message)
  //   //
  //   // (topic, partition) => Message[]
  //   // cache.query(topic, partition, offset)

  // }
}
