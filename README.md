# Local Kafka Service

Local Kafka Service is a fun exercise of a single-node kafka compatabile server that persists messages to S3. For the purpose

## Control Plane
### Creating a Topic
The majority of the code is revolves around producing & fetching messages for a single topic. However, for consistency's sake, this server has an endpoint that creates a topic with a default 100 partitions. The service doesnt not support configuring this setting and 

```bash
curl -X POST http://localhost:8000/topics -H "Content-Type: application/json" -d '{"name":"hello"}'
```
Request: { 200: Topic}
Response: (409: TopicAlreadyExistsException)

## Data Plane
### Producing Messages

Note: Messages need to be less than 1Kb.

```bash
CURL -X POST http://localhost:8123/topics/hello/produce/0 -d "key:value"
```

- partition segment (offset and not duplicating data)
- 


### Querying Messages


### UML Diagram


## Compaction

## Failure Scenarios
- slow flush beats newer flush()
- flushing on size instead of time

## Testing

## Distributed System
#### Metadata Service -> Strongly Consistent Commits
#### FileStorage Service -> S3
##### latency -> 100ms
#### Maintaining Offset -> Hash Request to Same Agent
#### Heartbeat Monitoring -> Metadata Service
#### Re-routing request (similar to Redis)


- nvm use 18.18
- pnpm run test
- pnpm run start:dev

## Installation

```bash
$ pnpm install
```

## Running the app

```bash
# development
$ pnpm run start

# watch mode
$ pnpm run start:dev

# production mode
$ pnpm run start:prod
```

## Test

```bash
# unit tests
$ pnpm run test

# e2e tests
$ pnpm run test:e2e

# test coverage
$ pnpm run test:cov
```

## Support

Nest is an MIT-licensed open source project. It can grow thanks to the sponsors and support by the amazing backers. If you'd like to join them, please [read more here](https://docs.nestjs.com/support).

## Stay in touch

- Author - [Kamil Myśliwiec](https://kamilmysliwiec.com)
- Website - [https://nestjs.com](https://nestjs.com/)
- Twitter - [@nestframework](https://twitter.com/nestframework)

## License

Nest is [MIT licensed](LICENSE).
