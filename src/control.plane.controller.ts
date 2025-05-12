import { Body, Controller, Post, Res } from '@nestjs/common';
import { ControlPlaneService } from './control.plane.service';
import { ApiResponse } from '@nestjs/swagger';
import Topic from './types/topic';
import { IsString } from 'class-validator';
import { Response } from 'express';

export class CreateTopicRequest {
  @IsString()
  name: string;
}

@Controller()
export class ControlPlaneController {
  constructor(private readonly controlPlaneService: ControlPlaneService) {}

  @Post('/topics')
  @ApiResponse({
    status: 200,
    type: Topic,
  })
  createTopic(
    @Body() input: CreateTopicRequest,
    @Res() response: Response,
  ): any {
    const topic: Topic = this.controlPlaneService.createTopic(input.name);
    response.json(topic);
  }
}
