import { Test, TestingModule } from '@nestjs/testing';
import { ControlPlaneService } from '../src/control.plane.service';
import { PartitionCommit } from '../src/segment/types';

describe('ControlPlaneService', () => {
    let service: ControlPlaneService;

    beforeEach(async () => {
        const module: TestingModule = await Test.createTestingModule({
            providers: [
                ControlPlaneService,
                {
                    provide: 'PARTITION_COUNT',
                    useValue: 3,
                },
            ],
        }).compile();

        service = module.get<ControlPlaneService>(ControlPlaneService);
    });

    describe('findPosition', () => {
        it('should return 0 when there are no commits for the partition', () => {
            // const position = service.findPosition(0, 10);
            // expect(position).toBe(0);
            console.log("todo")
        });

        // it('should find position when offset is equal to a commit offset', () => {
        //     // Setup test data directly in the service's private commits Map
        //     const commits: PartitionCommit[] = [
        //         { partitionId: 1, offset: 5, position: 100 },
        //         { partitionId: 1, offset: 10, position: 200 },
        //         { partitionId: 1, offset: 15, position: 300 },
        //     ];

        //     // Set the commits directly in the service's private Map
        //     (service as any).commits.set(1, commits);

        //     const position = service.findPosition(1, 10);
        //     expect(position).toBe(200);
        // });

        // it('should find position when offset is between commit offsets', () => {
        //     const commits: PartitionCommit[] = [
        //         { partitionId: 2, offset: 5, position: 100 },
        //         { partitionId: 2, offset: 10, position: 200 },
        //         { partitionId: 2, offset: 15, position: 300 },
        //     ];

        //     (service as any).commits.set(2, commits);

        //     const position = service.findPosition(2, 12);
        //     expect(position).toBe(200); // Should return position of offset 10
        // });

        // it('should find position when offset is higher than all commits', () => {
        //     const commits: PartitionCommit[] = [
        //         { partitionId: 1, offset: 5, position: 100 },
        //         { partitionId: 1, offset: 10, position: 200 },
        //     ];

        //     (service as any).commits.set(1, commits);

        //     const position = service.findPosition(1, 20);
        //     expect(position).toBe(200); // Should return position of the highest offset
        // });

        // it('should find position when offset is lower than all commits', () => {
        //     const commits: PartitionCommit[] = [
        //         { partitionId: 1, offset: 5, position: 100 },
        //         { partitionId: 1, offset: 10, position: 200 },
        //     ];

        //     (service as any).commits.set(1, commits);

        //     const position = service.findPosition(1, 3);
        //     expect(position).toBe(0); // Should return 0 as offset is lower than all commits
        // });

        // it('should handle case with only one commit', () => {
        //     const commits: PartitionCommit[] = [
        //         { partitionId: 0, offset: 5, position: 100 },
        //     ];

        //     (service as any).commits.set(0, commits);

        //     const position = service.findPosition(0, 7);
        //     expect(position).toBe(100);
        // });
    });
});
