import { Injectable } from '@nestjs/common';
import Topic from './types/topic';
import { TopicAlreadyExistsException } from './exceptions';
import * as fs from 'fs';
import * as path from 'path';
import generateId from './lib/generateId';
import { plainToInstance } from 'class-transformer';

// interface CommitRecord {
//     id: string;
//     topic: string;
//     partition: number;
//     fileKey: string;
//     timestamp: Date;
//     messageCount: number;
// }

@Injectable()
export class ControlPlaneService {
  private readonly topics = new Set<Topic>();
  // private readonly commits: CommitRecord[] = [];
  private readonly dataDir = `/tmp/sks`;
  private readonly topicsPath: string;
  private readonly commitsPath: string;

  constructor() {
    this.topicsPath = path.join(this.dataDir, 'topics.json');
    this.commitsPath = path.join(this.dataDir, 'commits.json');

    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir, { recursive: true });
    }

    this.loadTopics();
    // this.loadCommits();
  }

  private loadTopics(): void {
    if (fs.existsSync(this.topicsPath)) {
      try {
        const data: any[] = JSON.parse(
          fs.readFileSync(this.topicsPath, 'utf8'),
        );
        const instances: Topic[] = plainToInstance(Topic, data, {
          enableImplicitConversion: true,
        });
        instances.forEach((topic) => this.topics.add(topic));
      } catch (error) {
        console.log('Failed to load topics:', error);
      }
    }
  }

  // private loadCommits(): void {
  //     if (fs.existsSync(this.commitsPath)) {
  //         try {
  //             this.commits.push(...JSON.parse(fs.readFileSync(this.commitsPath, 'utf8')));
  //         } catch (error) {
  //             console.log('Failed to load commits:', error);
  //         }
  //     }
  // }

  /**
   * Returns topic.
   * @param name
   * @returns
   */
  getTopic(name: string): Topic {
    return Array.from(this.topics).find((topic) => topic.name === name);
  }

  /**
   * Create topic.
   * @param name
   * @returns
   */
  createTopic(name: string): Topic {
    let topic = this.getTopic(name);
    if (topic) {
      throw new TopicAlreadyExistsException();
    }

    topic = new Topic();
    topic.id = generateId('topic');
    topic.name = name;
    topic.createdTimestamp = new Date();
    topic.updatedTimestamp = new Date();

    this.topics.add(topic);
    this.saveTopics();

    return topic;
  }

  /**
   * Commit a file to metadata
   * @param topic The topic name
   * @param partition The partition ID
   * @param file The storage file metadata
   */
  async commit(topic: string, partition: number, file: any): Promise<void> {
    // Create a commit record
    // const commitRecord: CommitRecord = {
    //     id: generateId('commit'),
    //     topic,
    //     partition,
    //     fileKey: file.key,
    //     timestamp: new Date(),
    //     messageCount: file.size // This should be the actual message count
    // };
    // // Add to commits array
    // this.commits.push(commitRecord);
    // Save commits to disk
    // await this.saveCommits();
  }

  private async saveTopics(): Promise<void> {
    await fs.promises.writeFile(
      this.topicsPath,
      JSON.stringify(Array.from(this.topics), null, 2),
    );
  }

  // private async saveCommits(): Promise<void> {
  //     await fs.promises.writeFile(
  //         this.commitsPath,
  //         JSON.stringify(this.commits, null, 2)
  //     );
  // }
}
