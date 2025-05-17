import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { ValidationPipe } from '@nestjs/common';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { NodeSDK, logs } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';

async function bootstrap() {
  // const sdk = new NodeSDK({
  //   traceExporter: new OTLPTraceExporter(),
  //   instrumentations: [
  //     getNodeAutoInstrumentations({
  //       // we recommend disabling fs autoinstrumentation since it can be noisy
  //       // and expensive during startup
  //       '@opentelemetry/instrumentation-fs': {
  //         enabled: false,
  //       },
  //     }),
  //   ],
  // });
  // sdk.start();

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
