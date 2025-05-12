
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
    // most elegant way to keep track of index?
    // (topic, partition) -> BufferedMessage[]
    //   private messages: Array<BufferedMessage> = [];
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
        // const message = new BufferedMessage(data, callback, topic, partition);
        // this.messages.push(message);
        // this.sizeInBytes += message.getMessageSize();
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
        return 0
        // return this.messages.length;
    }


    /**
     * Resolve all message callbacks in the batch
     */
    resolveAll(): void {
        // for (const message of this.messages) {
        //   message.resolve();
        // }
        // this.messages = [];
        this.sizeInBytes = 0;
    }

    /**
     * Reject all message callbacks in the batch
     * @param error The error to pass to the callback
     */
    rejectAll(error?: any): void {
        // for (const message of this.messages) {
        //   message.reject(error);
        // }
        // this.messages = [];
        this.sizeInBytes = 0;
    }
}
