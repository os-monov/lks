/**
 * Represents a message.
 */
export class Message {

    constructor(
        private readonly size: number,
        private readonly offset: number,
        private readonly data: string,
        // private
    ) {
        this.size = size;
        this.offset = offset;
        this.data = data;
    }

    serialize(): string {
        return `${this.size}:${this.offset}:${this.data}`;
    }
}
