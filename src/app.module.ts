import { Module } from '@nestjs/common';
import { ControlPlaneService } from './control.plane.service';
import { AppController } from './app.controller';
import { RecordCache } from './record/record.cache';
import { ConfigModule } from '@nestjs/config';
import { MetricsService } from './metrics.service';
import { ConsoleLogger } from './console.logger';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
    }),
  ],
  controllers: [AppController],
  providers: [
    ControlPlaneService,
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
