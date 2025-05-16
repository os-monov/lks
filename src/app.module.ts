import { Module } from '@nestjs/common';
import { ControlPlaneService } from './control.plane.service';
import { AppController } from './app.controller';
import { RecordLogManager } from './record/record.log.manager';
import { RecordCache } from './record/record.cache';

@Module({
  imports: [],
  controllers: [AppController],
  providers: [
    RecordLogManager,
    RecordCache,
    ControlPlaneService,
    {
      provide: 'PARTITION_COUNT',
      useValue: 100,
    },
    {
      provide: 'LOG_COUNT',
      useValue: 4,
    },
  ],
})
export class AppModule {}
