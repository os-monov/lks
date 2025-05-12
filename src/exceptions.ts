import { HttpException } from '@nestjs/common';

export class TopicAlreadyExistsException extends HttpException {
  constructor(message?: string) {
    super(message || 'TopicAlreadyExists', 409); // 409 Conflict
    this.name = 'TopicAlreadyExistsException';
  }
}

export class TopicNotFoundException extends HttpException {
  constructor(message?: string) {
    super(message || 'Topic not found', 404);
    this.name = 'TopicNotFoundException';
  }
}

export class MessageSizeLimitException extends HttpException {
  constructor(maxSize: number = 1024) {
    super(`Message size exceeds maximum allowed size of ${maxSize} bytes`, 413); // 413 Payload Too Large
    this.name = 'MessageSizeExceededException';
  }
}
