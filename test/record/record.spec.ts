import { Record } from '../../src/record/record';

describe('Record', () => {
  const offset = 123n;
  const key = 'testKey';
  const value = 'testValue';

  describe('constructor and getters', () => {
    it('should create a record with the provided values', () => {
      const record = new Record(offset, key, value);

      expect(record.getOffset()).toBe(offset);
      expect(record.getKey()).toBe(key);
      expect(record.getValue()).toBe(value);
    });

    it('should handle empty strings', () => {
      const emptyRecord = new Record(offset, '', '');

      expect(emptyRecord.getOffset()).toBe(offset);
      expect(emptyRecord.getKey()).toBe('');
      expect(emptyRecord.getValue()).toBe('');
    });

    it('should handle non-ASCII characters', () => {
      const unicodeRecord = new Record(offset, '你好', '世界');

      expect(unicodeRecord.getOffset()).toBe(offset);
      expect(unicodeRecord.getKey()).toBe('你好');
      expect(unicodeRecord.getValue()).toBe('世界');
    });
  });
});
