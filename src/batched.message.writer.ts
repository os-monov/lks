import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { MessageBatch } from './types/message.batch';
import { ObjectStorageService } from './object.storage.service';
import { ControlPlaneService } from './control.plane.service';
import { Message } from './types/message';

@Injectable()
export class BatchedMessageWriter implements OnModuleInit, OnModuleDestroy {
  private activeBatch: MessageBatch = new MessageBatch();
  private flushBatch: MessageBatch = new MessageBatch();
  private flushInterval: NodeJS.Timeout;
  private pendingFlush: Promise<void> | null = null;

  private readonly maxBufferSizeBytes = 4 * 1024 * 1024; // 4MB
  private readonly flushIntervalMs = 200;

  constructor(
    private readonly objectStorageService: ObjectStorageService,
    private readonly controlPlaneService: ControlPlaneService,
  ) {}

  onModuleInit() {
    this.flushInterval = setInterval(() => this.flush(), this.flushIntervalMs);
  }

  async onModuleDestroy() {
    clearInterval(this.flushInterval);

    // Force a final flush of any pending messages
    if (this.activeBatch.getSize() > 0) {
      console.log('Flushing remaining messages before shutdown');
      await this.flush();
    }

    // Wait for any ongoing flush to complete
    if (this.pendingFlush) {
      await this.pendingFlush;
    }
  }

  /**
   * Append a message to the active batch
   * @param message The message to append
   * @param topic The topic name
   * @param partition The partition ID
   * @returns A promise that resolves when the message is successfully processed
   */
  write(message: any, topic: string, partition: number): Promise<void> {
    // // limit message to 1KB
    // const messageSize = Buffer.from(JSON.stringify(input)).length;
    // if (messageSize > AppController.MAX_MESSAGE_SIZE_BYTES) {
    //   throw new MessageSizeLimitException();
    // }

    // Trigger flush if we've reached the size limit
    if (this.activeBatch.getSizeBytes() >= this.maxBufferSizeBytes) {
      this.flush();
    }

    return new Promise<void>((resolve, reject) => {
      // Add message to the batch with callbacks for resolution
      this.activeBatch.addMessage(
        message,
        { resolve, reject },
        topic,
        partition,
      );
    });
  }

  /**
   * Flush the active batch to storage
   * @returns A promise that resolves when the flush is complete
   */
  private flush(): Promise<void> {
    // Skip if no messages to flush
    if (this.activeBatch.getSize() === 0) {
      return Promise.resolve();
    }

    // Swap the active and flush batches
    this.flushBatch = this.activeBatch;
    this.activeBatch = new MessageBatch();

    // Track the pending flush
    this.pendingFlush = this.performFlush();
    return this.pendingFlush;
  }

  private async performFlush(): Promise<void> {
    try {
      // Serialize the batch
      const serializedBatch = this.flushBatch.serialize();

      // Store the batch and get the file reference
      const file = await this.objectStorageService.putObject(
        serializedBatch.data,
        serializedBatch.topic,
        serializedBatch.partition,
      );

      // Update metadata with the file location
      await this.controlPlaneService.commit(
        serializedBatch.topic,
        serializedBatch.partition,
        file,
      );

      // Resolve all promises for the messages in this batch
      this.flushBatch.resolveAll();

      // Clear the pending flush reference
      this.pendingFlush = null;
    } catch (error) {
      // Reject all promises for the messages in this batch
      this.flushBatch.rejectAll(error);

      // Clear the pending flush reference
      this.pendingFlush = null;

      // Re-throw the error
      throw error;
    }
  }
}
