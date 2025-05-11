import { Test, TestingModule } from '@nestjs/testing';
import { AppController } from './app.controller';
import { MetadataService } from './metadata.service';

describe('AppController', () => {
  let appController: AppController;

  beforeEach(async () => {
    const app: TestingModule = await Test.createTestingModule({
      controllers: [AppController],
      providers: [MetadataService],
    }).compile();

    appController = app.get<AppController>(AppController);
  });
});
