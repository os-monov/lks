import { Injectable } from '@nestjs/common';
import Topic from './types/topic';
import { TopicAlreadyExistsException } from './exceptions';
import * as fs from 'fs';
import * as path from 'path';
import generateId from './lib/generateId';
import { plainToInstance } from 'class-transformer';

@Injectable()
export class MetadataService {
  private readonly topics = new Set<Topic>();

  constructor() {
    const dir = `/tmp/sks`;
    const topicsPath = path.join(dir, 'topics.json');

    if (fs.existsSync(topicsPath)) {
      try {
        const data: any[] = JSON.parse(fs.readFileSync(topicsPath, 'utf8'));

        const instances: Topic[] = plainToInstance(Topic, data, {
          enableImplicitConversion: true,
        });
        instances.forEach((topic) => this.topics.add(topic));
      } catch {
        console.log('Failed to load topics.');
      }
    }
  }

  createTopic(name: string) {
    let topic = Array.from(this.topics).find((topic) => topic.name);
    if (topic) {
      throw new TopicAlreadyExistsException();
    }

    topic = new Topic();
    topic.id = generateId('topic');
    topic.name = name;
    topic.createdTimestamp = new Date();
    topic.updatedTimestamp = new Date();

    this.topics.add(topic);

    // save to disk to mimic db write
    const dir = `/tmp/sks`;
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    const topicsPath = path.join(dir, 'topics.json');
    fs.writeFileSync(
      topicsPath,
      JSON.stringify(Array.from(this.topics), null, 2),
    );

    return topic;
  }

  commit() {
    // use lock
  }

  init() {}

  save() {
    // flush topics to topics.json
    // flush commits to commits.json
  }
}
