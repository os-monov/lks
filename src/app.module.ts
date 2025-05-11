import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { MetadataService } from './metadata.service';

@Module({
  imports: [],
  controllers: [AppController],
  providers: [MetadataService],
})
export class AppModule {}
