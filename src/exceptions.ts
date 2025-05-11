import { HttpException } from '@nestjs/common';

export class TopicAlreadyExistsException extends HttpException {
  constructor(message?: string) {
    super(message || 'TopicAlreadyExists', 409); // 409 Conflict
    this.name = 'TopicAlreadyExistsException';
  }
}
