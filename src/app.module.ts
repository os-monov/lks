import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { RecordCache } from './record/record.cache';
import { ConfigModule } from '@nestjs/config';
import { MetricsService } from './metrics.service';
import { ConsoleLogger } from './console.logger';
import { ScheduleModule } from '@nestjs/schedule';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
    }),
    ScheduleModule.forRoot(),
  ],
  controllers: [AppController],
  providers: [
    RecordCache,
    ConsoleLogger,
    MetricsService,
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
