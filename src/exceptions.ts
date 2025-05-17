import { HttpException } from '@nestjs/common';

export class MessageSizeLimitException extends HttpException {
  constructor(maxSize: number = 1024) {
    super(`Message size exceeds maximum allowed size of ${maxSize} bytes`, 413); // 413 Payload Too Large
    this.name = 'MessageSizeExceededException';
  }
}

export class PartitionNotFoundException extends HttpException {
  constructor() {
    super(`Partition does not exist.`, 400); // Invalid Input
    this.name = 'PartitionNotFoundException';
  }
}

export class InternalServerException extends HttpException {
  constructor(message?: string) {
    super('InternalServerException', 500);
    this.name = 'InternalServerException';
    this.message = message;
  }
}

export class InvalidRecordException extends HttpException {
  constructor() {
    super('InvalidRecordException', 400);
    this.name = 'InvalidRecordException';
  }
}
