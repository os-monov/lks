import { Injectable } from '@nestjs/common';

@Injectable()
export class ConsoleLogger {
    constructor() { }

    public info(message: string) {
        console.log(`[INFO] [${new Date()}] ${message}`);
    }

    public error(message: string) {
        console.log(`[ERROR] [${new Date()}] ${message}`);
    }
}
