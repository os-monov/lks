import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { ValidationPipe } from '@nestjs/common';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  // validates all in-bound requests
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true, // strips in-bound requests
      transform: true, // types in-bound requests to validation class
    }),
  );

  // --bucket mock-kafka-bucket
  // --max-batch-size (MB) 4
  // --max-batch-duration 200ms

  await app.listen(8123);
}
bootstrap();
