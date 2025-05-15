import { Offset } from './types';

export class Record {
  constructor(
    private readonly offset: Offset,
    private readonly key: string,
    private readonly value: string,
  ) {}
}
