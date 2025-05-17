import { ControlPlaneService } from '../src/control.plane.service';

describe('ControlPlaneService', () => {
  let service: ControlPlaneService;

  beforeEach(() => {
    // Provide a dummy file path and partition count
    service = new ControlPlaneService('/tmp/non-existent.ndjson', 3);
  });

  function setCommits(
    partitionId: number,
    commits: { offset: bigint; position: number }[],
  ) {
    (service as any).commits.set(
      partitionId,
      commits.map(({ offset, position }) => ({
        partitionId,
        offset,
        position,
      })),
    );
  }

  describe('findPosition', () => {
    it('returns 0 when there are no commits for the partition', () => {
      expect(service.findPosition(0, 10n)).toBe(0);
    });

    it('returns correct position when offset equals a commit offset', () => {
      setCommits(1, [
        { offset: 5n, position: 100 },
        { offset: 10n, position: 200 },
        { offset: 15n, position: 300 },
      ]);
      expect(service.findPosition(1, 10n)).toBe(200);
    });

    it('returns correct position when offset is between commit offsets', () => {
      setCommits(2, [
        { offset: 5n, position: 100 },
        { offset: 10n, position: 200 },
        { offset: 15n, position: 300 },
      ]);
      expect(service.findPosition(2, 12n)).toBe(200); // Should return position of offset 10
    });

    it('returns position of the highest offset when offset is higher than all commits', () => {
      setCommits(1, [
        { offset: 5n, position: 100 },
        { offset: 10n, position: 200 },
      ]);
      expect(service.findPosition(1, 20n)).toBe(200); // Should return position of the highest offset
    });

    it('returns 0 when offset is lower than all commits', () => {
      setCommits(1, [
        { offset: 5n, position: 100 },
        { offset: 10n, position: 200 },
      ]);
      expect(service.findPosition(1, 3n)).toBe(0);
    });

    it('returns correct position with only one commit', () => {
      setCommits(0, [{ offset: 5n, position: 100 }]);
      expect(service.findPosition(0, 7n)).toBe(100);
      expect(service.findPosition(0, 5n)).toBe(100);
      expect(service.findPosition(0, 3n)).toBe(0);
    });
  });
});
