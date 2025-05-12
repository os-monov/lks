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

  await app.listen(8123);
}
bootstrap();
