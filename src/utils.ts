import * as path from 'path';
import * as fs from 'fs';

export class Utils {
    private static readonly metricsFilePath = path.join('/tmp/lks', 'metrics.csv');

    public static emit(name: string, value: number) {
        const row = `${Date.now()},${name},${value}\n`;
        fs.appendFileSync(this.metricsFilePath, row);
    }
}