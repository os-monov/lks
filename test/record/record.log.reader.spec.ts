import * as path from 'path';
import { RecordLogReader } from '../../src/record/record.log.reader';

describe('RecordLogReader.scan', () => {
    it('prints segment summaries for sample.log', () => {
        const sampleLog = path.resolve(__dirname, '../data/sample.log');
        const reader = new RecordLogReader(sampleLog);

        // const printed: string[] = [];
        // const spy = jest
        //     .spyOn(console, 'log')
        //     .mockImplementation((msg: unknown) => printed.push(String(msg)));

        reader.scan();

        // spy.mockRestore();

        // // basic expectations
        // expect(printed.length).toBeGreaterThan(0);
        // printed.forEach((line) =>
        //     expect(line).toMatch(
        //         /\[\d+\] partition=\d+ offset=\d+ records=\d+ payload=\d+B/,
        //     ),
        // );
    });
});
