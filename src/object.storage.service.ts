import { Injectable } from '@nestjs/common';
import * as fs from 'fs';
import * as path from 'path';
import generateId from './lib/generateId';

@Injectable()
export class ObjectStorageService {
  private readonly basePath: string;

  constructor() {
    this.basePath = '/tmp/sks/storage';

    // Ensure storage directory exists
    if (!fs.existsSync(this.basePath)) {
      fs.mkdirSync(this.basePath, { recursive: true });
    }
  }

  /**
   * Store object data to storage
   * @param data The data buffer to store
   * @param topic The topic name
   * @param partition The partition ID
   * @returns StorageFile object with metadata
   */
  async putObject(
    data: Buffer,
    topic: string,
    partition: number,
  ): Promise<void> {
    // Generate a unique file key
    const id = generateId('file');
    const fileKey = `${topic}/${partition}/${id}`;
    const filePath = path.join(this.basePath, fileKey);

    // Ensure directory exists
    const dirPath = path.dirname(filePath);
    if (!fs.existsSync(dirPath)) {
      fs.mkdirSync(dirPath, { recursive: true });
    }

    // Write file
    await fs.promises.writeFile(filePath, data);
  }

  /**
   * Retrieve object data from storage
   * @param key The storage key
   * @returns Buffer containing the data
   */
  async getObject(key: string): Promise<Buffer> {
    const filePath = path.join(this.basePath, key);
    return fs.promises.readFile(filePath);
  }
}
