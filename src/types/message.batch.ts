import { BufferedMessage } from './buffered.message';

export interface MessageCallback {
  resolve: () => void;
  reject: (error?: any) => void;
}

export interface StorageFile {
  topic: string;
  partition: number;
  path: string;
  messageCount: number;
}

export class MessageBatch {
  private messages: Array<BufferedMessage> = [];
  private sizeInBytes = 0;

  /**
   * Add a message to the batch
   * @param data The message data
   * @param callback The callback to invoke when the message is processed
   * @param topic The topic name
   * @param partition The partition ID
   */
  addMessage(
    data: any,
    callback: MessageCallback,
    topic: string,
    partition: number,
  ): void {
    const message = new BufferedMessage(data, callback, topic, partition);
    this.messages.push(message);
    this.sizeInBytes += message.getMessageSize();
  }

  /**
   * Get the total size of the batch in bytes
   */
  getSizeBytes(): number {
    return this.sizeInBytes;
  }

  /**
   * Get the number of messages in the batch
   */
  getSize(): number {
    return this.messages.length;
  }

  /**
   * Serialize the batch for storage
   */
  serialize(): { data: Buffer; topic: string; partition: number } {
    // Group messages by topic and partition
    const messagesByGroup = this.messages.reduce(
      (groups, message) => {
        const key = `${message.getTopic()}-${message.getPartition()}`;
        if (!groups[key]) {
          groups[key] = {
            topic: message.getTopic(),
            partition: message.getPartition(),
            messages: [],
          };
        }
        groups[key].messages.push(message.serialize());
        return groups;
      },
      {} as Record<
        string,
        { topic: string; partition: number; messages: any[] }
      >,
    );

    // For simplicity, we'll use the first group
    // In a real implementation, you'd handle multiple topics/partitions
    const firstGroup = Object.values(messagesByGroup)[0];
    if (!firstGroup) {
      throw new Error('No messages to serialize');
    }

    // Serialize messages to a Buffer
    const serializedData = Buffer.from(JSON.stringify(firstGroup.messages));

    return {
      data: serializedData,
      topic: firstGroup.topic,
      partition: firstGroup.partition,
    };
  }

  /**
   * Resolve all message callbacks in the batch
   */
  resolveAll(): void {
    for (const message of this.messages) {
      message.resolve();
    }
    this.messages = [];
    this.sizeInBytes = 0;
  }

  /**
   * Reject all message callbacks in the batch
   * @param error The error to pass to the callback
   */
  rejectAll(error?: any): void {
    for (const message of this.messages) {
      message.reject(error);
    }
    this.messages = [];
    this.sizeInBytes = 0;
  }
}
