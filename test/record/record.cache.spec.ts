import { Offset } from '../../src/segment/types';
import { RecordCache } from '../../src/record/record.cache';
import { Record } from '../../src/record/record';

describe('RecordCache - Intensive', () => {
  let cache: RecordCache;

  function makeRecord(offset: Offset, key: string, value: string) {
    return new Record(offset, key, value);
  }

  beforeEach(() => {
    cache = new RecordCache();
  });

  it('handles duplicate offsets by last-in-wins (SortedSet behavior)', () => {
    cache.insert(0, [makeRecord(10n, 'a', 'x')]);
    cache.insert(0, [makeRecord(10n, 'b', 'y')]);
    const records = cache.get(0);
    console.log(records);
    expect(records.length).toBe(1); // Only one record at offset 10
    expect(records[0].getOffset()).toBe(10n);
    // last write wins
    expect(records[0].getKey()).toBe('b');
    expect(records[0].getValue()).toBe('y');
  });

  it('handles very large BigInt offsets', () => {
    const bigOffset = 2n ** 62n;
    cache.insert(0, [makeRecord(bigOffset, 'big', 'int')]);
    expect(cache.get(0)[0].getOffset()).toBe(bigOffset);
    expect(cache.latestOffset(0)).toBe(bigOffset);
  });

  it('keeps offsets sorted even with random insertion order', () => {
    const offsets = [1000n, 1n, 50n, 500n, 100n, 2n];
    cache.insert(
      0,
      offsets.map((o) => makeRecord(o, `k${o}`, `v${o}`)),
    );
    const sortedOffsets = cache.get(0).map((r) => r.getOffset());
    expect(sortedOffsets).toEqual([1n, 2n, 50n, 100n, 500n, 1000n]);
  });

  it('supports multiple partitions with interleaved inserts', () => {
    for (let i = 0; i < 10; ++i) {
      cache.insert(i % 2, [makeRecord(BigInt(i), `k${i}`, `v${i}`)]);
    }
    expect(cache.get(0).length).toBe(5);
    expect(cache.get(1).length).toBe(5);
    expect(cache.get(0).map((r) => r.getOffset())).toEqual([
      0n,
      2n,
      4n,
      6n,
      8n,
    ]);
    expect(cache.get(1).map((r) => r.getOffset())).toEqual([
      1n,
      3n,
      5n,
      7n,
      9n,
    ]);
  });

  it('can handle thousands of records efficiently', () => {
    const count = 2000;
    const records = [];
    for (let i = 0; i < count; ++i) {
      records.push(makeRecord(BigInt(i), `k${i}`, `v${i}`));
    }
    cache.insert(0, records);
    expect(cache.get(0).length).toBe(count);
    expect(cache.latestOffset(0)).toBe(BigInt(count - 1));
    // Check order on a random sample
    const sample = cache.get(0)[1234];
    expect(sample.getOffset()).toBe(1234n);
  });

  it('does not mutate returned arrays', () => {
    cache.insert(0, [makeRecord(1n, 'a', 'A'), makeRecord(2n, 'b', 'B')]);
    const arr = cache.get(0);
    arr.push(makeRecord(3n, 'c', 'C'));
    // The cache should not be affected
    expect(cache.get(0).length).toBe(2);
  });

  it('latestOffset returns 0n for empty partition, even after heavy use', () => {
    for (let i = 0; i < 100; ++i) {
      cache.insert(1, [makeRecord(BigInt(i), `k${i}`, `v${i}`)]);
    }
    expect(cache.latestOffset(0)).toBe(0n);
    expect(cache.get(0)).toEqual([]);
  });

  it('can insert same record object twice (does not duplicate)', () => {
    const record = makeRecord(123n, 'key', 'val');
    cache.insert(0, [record]);
    cache.insert(0, [record]);
    expect(cache.get(0).length).toBe(1);
    expect(cache.get(0)[0].getKey()).toBe('key');
  });
});
