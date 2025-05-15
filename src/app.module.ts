import { Module } from '@nestjs/common';
import { ControlPlaneService } from './control.plane.service';
import { AppController } from './app.controller';

@Module({
  imports: [],
  controllers: [AppController],
  providers: [ControlPlaneService],
})
export class AppModule {}
