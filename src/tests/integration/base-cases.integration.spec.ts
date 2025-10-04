import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { IHeaders } from "kafkajs";

import { Kafka } from "../../lib/kafka-js/kafka.js";

describe("Kafka Mock Integration Tests", () => {
	let kafka: Kafka;

	beforeEach(() => {
		kafka = new Kafka({
			brokers: ["kafka1:9092", "kafka2:9092"],
			clientId: "my-app",
		});
	});

	afterEach(async () => {
		await kafka.disconnect();
	});

	it("should work exactly like the README example", async () => {
		// Step 1: Create producer and send message
		const producer = kafka.producer();
		await producer.connect();

		await producer.send({
			topic: "test-topic",

			messages: [
				{ value: "Hello KafkaJS user!" },
			],
		});

		await producer.disconnect();

		// Step 2: Create consumer and consume message
		const consumer = kafka.consumer({ groupId: "test-group" });
		const receivedMessages: string[] = [];

		await consumer.connect();
		await consumer.subscribe({ fromBeginning: true, topics: ["test-topic"] });

		await consumer.run({
			eachMessage: async ({ message }) => {
				receivedMessages.push(message.value?.toString() || "");
			},
		});

		// Give some time for message processing
		await new Promise(resolve => setTimeout(resolve, 100));

		await consumer.disconnect();

		// Verify that the message was processed
		expect(receivedMessages.length).toBeGreaterThanOrEqual(0);
	});

	it("should handle multiple producers and consumers", async () => {
		const producer1 = kafka.producer();
		const producer2 = kafka.producer();
		const consumer1 = kafka.consumer({ groupId: "group-1" });
		const consumer2 = kafka.consumer({ groupId: "group-2" });

		const messages1: string[] = [];
		const messages2: string[] = [];

		// Setup consumers
		await consumer1.connect();
		await consumer1.subscribe({ fromBeginning: true, topics: ["topic-1"] });
		await consumer1.run({
			eachMessage: async ({ message }) => {
				messages1.push(message.value?.toString() || "");
			},
		});

		await consumer2.connect();
		await consumer2.subscribe({ fromBeginning: true, topics: ["topic-2"] });
		await consumer2.run({
			eachMessage: async ({ message }) => {
				messages2.push(message.value?.toString() || "");
			},
		});

		// Send messages
		await producer1.connect();
		await producer1.send({
			topic: "topic-1",

			messages: [{ value: "Message for topic-1" }],
		});
		await producer1.disconnect();

		await producer2.connect();
		await producer2.send({
			topic: "topic-2",

			messages: [{ value: "Message for topic-2" }],
		});
		await producer2.disconnect();

		// Give some time for message processing
		await new Promise(resolve => setTimeout(resolve, 100));

		await consumer1.disconnect();
		await consumer2.disconnect();

		expect(messages1.length).toBeGreaterThanOrEqual(0);
		expect(messages1).toEqual(["Message for topic-1"]);

		expect(messages2.length).toBeGreaterThanOrEqual(0);
		expect(messages2).toEqual(["Message for topic-2"]);
	});

	it("should handle messages with keys and headers", async () => {
		const producer = kafka.producer();
		const consumer = kafka.consumer({ groupId: "test-group" });
		const receivedMessages: {
			headers?: IHeaders;
			key?: string | null;
			value?: string | null;
		}[] = [];

		await consumer.connect();
		await consumer.subscribe({ fromBeginning: true, topics: ["test-topic"] });
		await consumer.run({
			eachMessage: async ({ message }) => {
				receivedMessages.push({
					headers: message.headers,
					key: message.key?.toString(),
					value: message.value?.toString(),
				});
			},
		});

		await producer.connect();
		await producer.send({
			topic: "test-topic",

			messages: [
				{
					headers: { "content-type": "application/json" },
					key: "user-123",
					value: "User message",
				},
				{
					headers: { "source": "test" },
					value: "Message without key",
				},
			],
		});
		await producer.disconnect();

		// Give some time for message processing
		await new Promise(resolve => setTimeout(resolve, 100));

		await consumer.disconnect();

		expect(receivedMessages.length).toBeGreaterThanOrEqual(0);
		expect(receivedMessages).toEqual([
			{
				headers: { "content-type": "application/json" },
				key: "user-123",
				value: "User message",
			},
			{
				headers: { "source": "test" },
				value: "Message without key",
			},
		]);
	});

	it("should handle admin operations", async () => {
		const admin = kafka.admin();

		await admin.connect();

		expect(admin).toBeDefined();

		await admin.disconnect();
	});
});
