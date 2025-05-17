import * as path from 'path';
import { RecordLogReader } from '../../src/record/record.log.reader';
import { Record } from '../../src/record/record';
import { MetricsService } from '../../src/metrics.service';
import { ConsoleLogger } from '../../src/console.logger';

describe('RecordLogReader', () => {
  const LOG_PATH = path.resolve(__dirname, '../data/sample.log');

  let reader: RecordLogReader;
  let mockMetricsService: MetricsService;
  let mockLogger: ConsoleLogger;

  beforeAll(() => {
    // Create mocks
    mockMetricsService = {
      emit: jest.fn(),
    } as unknown as MetricsService;

    mockLogger = {
      info: jest.fn(),
      error: jest.fn(),
    } as unknown as ConsoleLogger;

    reader = new RecordLogReader(LOG_PATH, mockMetricsService, mockLogger);
  });

  describe('query', () => {
    it('should query all records if position is 0', async () => {
      const records: Record[] = await reader.query(0, 0);
      expect(records.length).toBe(855);
      const first = records.at(0);
      expect(first).toBeDefined();
      expect(first.getOffset()).toBe(1n);
      expect(first.getKey()).toBe('05830844');
      expect(first.getValue()).toBe('Kathryn Champlin V');

      const last = records.at(records.length - 1);
      expect(last).toBeDefined();
      expect(last.getOffset()).toBe(855n);
      expect(last.getKey()).toBe('52358110');
      expect(last.getValue()).toBe('Miss Lana Von');
    });

    it('should query for records after provided position', async () => {
      const records: Record[] = await reader.query(0, 50000);
      expect(records.length).toBe(543);
      const first = records.at(0);
      expect(first).toBeDefined();
      expect(first.getOffset()).toBe(313n);
      expect(first.getKey()).toBe('25350118');
      expect(first.getValue()).toBe('Beth Johnson');
    });

    it('should return no records if position is out of bounds', async () => {
      const OUT_OF_BOUNDS = 1000000;
      const records: Record[] = await reader.query(0, OUT_OF_BOUNDS);
      expect(records.length).toBe(0);
    });
  });
});
