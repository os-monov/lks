import { Module } from '@nestjs/common';
import { ObjectStorageService } from './object.storage.service';
import { ControlPlaneController } from './control.plane.controller';
import { DataPlaneController } from './data.plane.controller';
import { ControlPlaneService } from './control.plane.service';
import { BatchedMessageWriter } from './batched.message.writer';
import { QueryService } from './query.service';

@Module({
  imports: [],
  controllers: [ControlPlaneController, DataPlaneController],
  providers: [
    ControlPlaneService,
    ObjectStorageService,
    BatchedMessageWriter,
    QueryService,
  ],
})
export class AppModule {}
