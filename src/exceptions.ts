import { HttpException } from '@nestjs/common';

export class MessageSizeLimitException extends HttpException {
  constructor(maxSize: number = 1024) {
    super(`Message size exceeds maximum allowed size of ${maxSize} bytes`, 413); // 413 Payload Too Large
    this.name = 'MessageSizeExceededException';
  }
}
