import { Injectable } from '@nestjs/common';
import * as fs from 'fs';
import * as path from 'path';
import { plainToInstance } from 'class-transformer';


@Injectable()
export class ControlPlaneService {
  private readonly dataDir = `/tmp/lks`;

  constructor() {
    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir, { recursive: true });
    }
  }
}
