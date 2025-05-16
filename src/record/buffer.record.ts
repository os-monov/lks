import { Record } from './record';
import { Offset } from '../segment/types';

export class BufferRecord extends Record {
  constructor(
    offset: Offset,
    key: string,
    value: string,
    private readonly timestamp: Date,
    private readonly callback: Function,
  ) {
    super(offset, key, value);
  }

  resolve(offset: Offset): void {
    this.callback(offset);
  }

  reject(error: Error): void {
    this.callback(error);
  }

  getTimestamp(): Date {
    return this.timestamp;
  }
}
