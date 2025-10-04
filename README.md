# kafkajs-mock

![ci-cd](https://github.com/JS-AK/kafkajs-mock/actions/workflows/ci-cd-master.yml/badge.svg)

Mock for KafkaJS library, designed for testing applications that use Apache Kafka.

## Installation

```bash
npm install @js-ak/kafkajs-mock
```

## Usage

### Mocking KafkaJS in Tests

To use this mock in your tests, you need to mock the `kafkajs` module:

```javascript
import { vi } from 'vitest';

// Mock the kafkajs module
vi.mock('kafkajs', async () => {
  const { Kafka: MockKafka } = await import('@js-ak/kafkajs-mock');
  const kafkajs = await vi.importActual('kafkajs');

  return { ...kafkajs, Kafka: MockKafka };
});
```

#### Complete Test Example

```javascript
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

// Mock kafkajs module
vi.mock('kafkajs', async () => {
  const { Kafka: MockKafka } = await import('@js-ak/kafkajs-mock');
  const kafkajs = await vi.importActual('kafkajs');
  return { ...kafkajs, Kafka: MockKafka };
});

import { Kafka } from 'kafkajs';

describe('My Kafka Service', () => {
  let kafka;
  let producer;
  let consumer;

  beforeEach(async () => {
    kafka = new Kafka({
      clientId: 'test-app',
      brokers: ['localhost:9092'],
    });

    producer = kafka.producer();
    consumer = kafka.consumer({ groupId: 'test-group' });

    await producer.connect();
    await consumer.connect();
  });

  afterEach(async () => {
    await producer.disconnect();
    await consumer.disconnect();
  });

  it('should send and receive messages', async () => {
    const testMessage = 'Hello, Kafka!';
    const receivedMessages = [];

    // Subscribe to topic
    await consumer.subscribe({
      topics: ['test-topic'],
      fromBeginning: true
    });

    // Start consuming
    await consumer.run({
      eachMessage: async ({ message }) => {
        receivedMessages.push(message.value.toString());
      },
    });

    // Send message
    await producer.send({
      topic: 'test-topic',
      messages: [{ value: testMessage }],
    });

    // Wait for message processing
    await new Promise(resolve => setTimeout(resolve, 100));

    expect(receivedMessages).toContain(testMessage);
  });
});
```

#### For Jest

```javascript
// jest.setup.js or in your test file
jest.mock('kafkajs', async () => {
  const { Kafka: MockKafka } = await import('@js-ak/kafkajs-mock');
  const kafkajs = await jest.requireActual('kafkajs');
  return { ...kafkajs, Kafka: MockKafka };
});
```

#### For Mocha/Chai

```javascript
// In your test setup
const { Kafka: MockKafka } = require('@js-ak/kafkajs-mock');
const kafkajs = require('kafkajs');

// Replace Kafka class
kafkajs.Kafka = MockKafka;
```

### Basic example

```javascript
const { Kafka } = require('@js-ak/kafkajs-mock');

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
});
```

### Producer

```javascript
const producer = kafka.producer();

await producer.connect();
await producer.send({
  topic: 'test-topic',
  messages: [
    { value: 'Hello KafkaJS user!' },
  ],
});

await producer.disconnect();
```

### Consumer

```javascript
const consumer = kafka.consumer({ groupId: 'test-group' });

await consumer.connect();
await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });

await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log({
      value: message.value.toString(),
    });
  },
});
```

## Testing

### Running tests

```bash
# Run all tests
npm test

# Run only unit tests
npm run test:unit

# Run only integration tests
npm run test:integration

# Run tests in watch mode
npm run test:watch

# Run tests with coverage
npm run test:coverage
```

### Test structure

- `*.unit.spec.ts` - unit tests for individual components
- `*.integration.spec.ts` - integration tests for complete scenarios

## API

### Kafka

Main class for creating Kafka client.

#### Methods

- `producer(config?)` - creates producer
- `consumer(config)` - creates consumer
- `admin()` - creates admin client
- `disconnect()` - disconnects from all connections

#### Producer Methods

- `connect()` - connects to Kafka
- `send(record)` - sends messages
- `disconnect()` - disconnects from Kafka

#### Consumer Methods

- `connect()` - connects to Kafka
- `subscribe(topics)` - subscribes to topics
- `run(config)` - starts message processing
- `disconnect()` - disconnects from Kafka

## Features

- Compatible with basic KafkaJS API for testing
- Supports core producer/consumer operations
- Perfect for unit and integration tests
- No real Kafka broker required
- Simplified implementation focused on testing scenarios

## Limitations

- Not a full KafkaJS replacement - only core functionality for testing
- No real Kafka protocol implementation
- Simplified message ordering and partitioning
- Limited error handling compared to real Kafka
- Some advanced features may not be available
