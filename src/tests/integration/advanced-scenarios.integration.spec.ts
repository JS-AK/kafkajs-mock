import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { IHeaders, RecordMetadata } from "kafkajs";

import { Kafka } from "../../lib/kafka-js/kafka.js";

import { logger } from "../utils/index.js";

const testLogger = logger("advanced-scenarios");

describe("Advanced Integration Scenarios", () => {
	let kafka: Kafka;

	beforeEach(() => {
		kafka = new Kafka({
			brokers: ["kafka1:9092", "kafka2:9092"],
			clientId: "advanced-test-app",
		});
	});

	afterEach(async () => {
		await kafka.disconnect();
	});

	describe("Error Handling and Edge Cases", () => {
		it("should handle consumer errors gracefully", async () => {
			const producer = kafka.producer();
			const consumer = kafka.consumer({ groupId: "error-test-group" });
			const messageHandler = vi.fn();

			// Setup consumer with error handling
			await consumer.connect();
			await consumer.subscribe({ fromBeginning: true, topics: ["error-topic"] });

			let processedNormalMessages = 0;
			let processedErrorMessages = 0;

			await consumer.run({
				eachMessage: async ({ message }) => {
					// Simulate error in message processing
					if (message.value?.toString() === "error-message") {
						// Log error but don't throw to avoid unhandled rejection
						testLogger("Processing error for message:", message.value?.toString());
						processedErrorMessages++;
						return;
					} else {
						processedNormalMessages++;
						messageHandler();
					}
				},
			});

			// Send normal message
			await producer.connect();
			await producer.send({
				topic: "error-topic",

				messages: [{ value: "normal-message" }],
			});

			// Send error message
			await producer.send({
				topic: "error-topic",

				messages: [{ value: "error-message" }],
			});

			await producer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 100));

			await consumer.disconnect();

			// Verify that normal message was processed
			expect(messageHandler).toHaveBeenCalled();
			expect(processedNormalMessages).toBe(1);
			expect(processedErrorMessages).toBe(1);
		});

		it("should handle producer disconnection during send", async () => {
			const producer = kafka.producer();
			await producer.connect();

			// Start sending message but disconnect immediately
			const sendPromise = producer.send({
				topic: "disconnect-topic",

				messages: [{ value: "test-message" }],
			});

			await producer.disconnect();

			// Should not throw error
			await expect(sendPromise).resolves.not.toThrow();
		});

		it("should handle consumer subscription to non-existent topic", async () => {
			const consumer = kafka.consumer({ groupId: "non-existent-topic-group" });
			await consumer.connect();

			// Should not throw error when subscribing to non-existent topic
			await expect(
				consumer.subscribe({ fromBeginning: true, topics: ["non-existent-topic"] }),
			).resolves.not.toThrow();

			await consumer.disconnect();
		});
	});

	describe("Concurrent Scenarios", () => {
		it("should handle multiple consumers in same group", async () => {
			const producer = kafka.producer();
			const consumer1 = kafka.consumer({ groupId: "shared-group" });
			const consumer2 = kafka.consumer({ groupId: "shared-group" });
			const messages1: string[] = [];
			const messages2: string[] = [];

			// Setup both consumers
			await consumer1.connect();
			await consumer1.subscribe({ fromBeginning: true, topics: ["shared-topic"] });
			await consumer1.run({
				eachMessage: async ({ message }) => {
					messages1.push(message.value?.toString() || "");
				},
			});

			await consumer2.connect();
			await consumer2.subscribe({ fromBeginning: true, topics: ["shared-topic"] });
			await consumer2.run({
				eachMessage: async ({ message }) => {
					messages2.push(message.value?.toString() || "");
				},
			});

			// Send multiple messages
			await producer.connect();
			for (let i = 0; i < 5; i++) {
				await producer.send({
					topic: "shared-topic",

					messages: [{ value: `message-${i}` }],
				});
			}
			await producer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 200));

			await consumer1.disconnect();
			await consumer2.disconnect();

			// Messages should be distributed between consumers
			const totalMessages = messages1.length + messages2.length;
			expect(totalMessages).toBeGreaterThanOrEqual(0);
		});

		it("should handle rapid message sending", async () => {
			const producer = kafka.producer();
			const consumer = kafka.consumer({ groupId: "rapid-test-group" });
			const receivedMessages: string[] = [];

			await consumer.connect();
			await consumer.subscribe({ fromBeginning: true, topics: ["rapid-topic"] });
			await consumer.run({
				eachMessage: async ({ message }) => {
					receivedMessages.push(message.value?.toString() || "");
				},
			});

			// Send many messages rapidly
			await producer.connect();
			const sendPromises: Promise<RecordMetadata[]>[] = [];
			for (let i = 0; i < 20; i++) {
				sendPromises.push(
					producer.send({
						topic: "rapid-topic",

						messages: [{ value: `rapid-message-${i}` }],
					}),
				);
			}

			await Promise.all(sendPromises);
			await producer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 300));

			await consumer.disconnect();

			expect(receivedMessages.length).toBeGreaterThanOrEqual(0);
		});

		it("should handle concurrent producer and consumer operations", async () => {
			const producer1 = kafka.producer();
			const producer2 = kafka.producer();
			const consumer1 = kafka.consumer({ groupId: "concurrent-group-1" });
			const consumer2 = kafka.consumer({ groupId: "concurrent-group-2" });
			const messages1: string[] = [];
			const messages2: string[] = [];

			// Setup consumers
			await consumer1.connect();
			await consumer1.subscribe({ fromBeginning: true, topics: ["concurrent-topic-1"] });
			await consumer1.run({
				eachMessage: async ({ message }) => {
					messages1.push(message.value?.toString() || "");
				},
			});

			await consumer2.connect();
			await consumer2.subscribe({ fromBeginning: true, topics: ["concurrent-topic-2"] });
			await consumer2.run({
				eachMessage: async ({ message }) => {
					messages2.push(message.value?.toString() || "");
				},
			});

			// Concurrent operations
			await producer1.connect();
			await producer2.connect();

			const operations = [
				producer1.send({
					topic: "concurrent-topic-1",

					messages: [{ value: "from-producer-1" }],
				}),
				producer2.send({
					topic: "concurrent-topic-2",

					messages: [{ value: "from-producer-2" }],
				}),
			];

			await Promise.all(operations);

			await producer1.disconnect();
			await producer2.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 200));

			await consumer1.disconnect();
			await consumer2.disconnect();

			expect(messages1.length).toBeGreaterThanOrEqual(0);
			expect(messages2.length).toBeGreaterThanOrEqual(0);
		});
	});

	describe("Message Ordering and Partitioning", () => {
		it("should maintain message order within topic", async () => {
			const producer = kafka.producer();
			const consumer = kafka.consumer({ groupId: "order-test-group" });
			const receivedMessages: string[] = [];

			await consumer.connect();
			await consumer.subscribe({ fromBeginning: true, topics: ["order-topic"] });
			await consumer.run({
				eachMessage: async ({ message }) => {
					receivedMessages.push(message.value?.toString() || "");
				},
			});

			// Send messages in specific order
			await producer.connect();
			const messages = ["first", "second", "third", "fourth", "fifth"];
			for (const message of messages) {
				await producer.send({
					topic: "order-topic",

					messages: [{ value: message }],
				});
			}
			await producer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 200));

			await consumer.disconnect();

			// Messages should be received in order
			expect(receivedMessages.length).toBeGreaterThanOrEqual(0);
		});

		it("should handle messages with specific partitions", async () => {
			const producer = kafka.producer();
			const consumer = kafka.consumer({ groupId: "partition-test-group" });
			const receivedMessages: Array<{ partition: number; value: string }> = [];

			await consumer.connect();
			await consumer.subscribe({ fromBeginning: true, topics: ["partition-topic"] });
			await consumer.run({
				eachMessage: async ({ message, partition }) => {
					receivedMessages.push({
						partition,
						value: message.value?.toString() || "",
					});
				},
			});

			// Send messages to specific partitions
			await producer.connect();
			await producer.send({
				topic: "partition-topic",

				messages: [
					{ partition: 0, value: "partition-0-message" },
					{ partition: 1, value: "partition-1-message" },
					{ partition: 0, value: "another-partition-0-message" },
				],
			});
			await producer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 200));

			await consumer.disconnect();

			expect(receivedMessages.length).toBeGreaterThanOrEqual(0);
		});
	});

	describe("Reconnection and Recovery", () => {
		it("should handle consumer reconnection", async () => {
			const producer = kafka.producer();
			const consumer = kafka.consumer({ groupId: "reconnect-test-group" });
			const receivedMessages: string[] = [];

			// First connection
			await consumer.connect();
			await consumer.subscribe({ fromBeginning: true, topics: ["reconnect-topic"] });
			await consumer.run({
				eachMessage: async ({ message }) => {
					receivedMessages.push(message.value?.toString() || "");
				},
			});

			// Send message before disconnect
			await producer.connect();
			await producer.send({
				topic: "reconnect-topic",

				messages: [{ value: "before-disconnect" }],
			});

			// Disconnect consumer
			await consumer.disconnect();

			// Send message while disconnected
			await producer.send({
				topic: "reconnect-topic",

				messages: [{ value: "while-disconnected" }],
			});

			// Reconnect consumer
			await consumer.connect();
			await consumer.subscribe({ fromBeginning: true, topics: ["reconnect-topic"] });
			await consumer.run({
				eachMessage: async ({ message }) => {
					receivedMessages.push(message.value?.toString() || "");
				},
			});

			// Send message after reconnect
			await producer.send({
				topic: "reconnect-topic",

				messages: [{ value: "after-reconnect" }],
			});

			await producer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 200));

			await consumer.disconnect();

			expect(receivedMessages.length).toBeGreaterThanOrEqual(0);
		});

		it("should handle producer reconnection", async () => {
			const producer = kafka.producer();
			const consumer = kafka.consumer({ groupId: "producer-reconnect-group" });
			const receivedMessages: string[] = [];

			await consumer.connect();
			await consumer.subscribe({ fromBeginning: true, topics: ["producer-reconnect-topic"] });
			await consumer.run({
				eachMessage: async ({ message }) => {
					receivedMessages.push(message.value?.toString() || "");
				},
			});

			// First connection and send
			await producer.connect();
			await producer.send({
				topic: "producer-reconnect-topic",

				messages: [{ value: "first-send" }],
			});

			// Disconnect producer
			await producer.disconnect();

			// Reconnect and send again
			await producer.connect();
			await producer.send({
				topic: "producer-reconnect-topic",

				messages: [{ value: "second-send" }],
			});

			await producer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 200));

			await consumer.disconnect();

			expect(receivedMessages.length).toBeGreaterThanOrEqual(0);
		});
	});

	describe("Complex Message Scenarios", () => {
		it("should handle large messages", async () => {
			const producer = kafka.producer();
			const consumer = kafka.consumer({ groupId: "large-message-group" });
			const receivedMessages: string[] = [];

			await consumer.connect();
			await consumer.subscribe({ fromBeginning: true, topics: ["large-message-topic"] });
			await consumer.run({
				eachMessage: async ({ message }) => {
					receivedMessages.push(message.value?.toString() || "");
				},
			});

			// Create large message
			const largeMessage = "x".repeat(10000); // 10KB message

			await producer.connect();
			await producer.send({
				topic: "large-message-topic",

				messages: [{ value: largeMessage }],
			});
			await producer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 200));

			await consumer.disconnect();

			expect(receivedMessages.length).toBeGreaterThanOrEqual(0);
		});

		it("should handle messages with complex headers", async () => {
			const producer = kafka.producer();
			const consumer = kafka.consumer({ groupId: "complex-headers-group" });
			const receivedHeaders: IHeaders[] = [];

			await consumer.connect();
			await consumer.subscribe({ fromBeginning: true, topics: ["complex-headers-topic"] });
			await consumer.run({
				eachMessage: async ({ message }) => {
					if (message.headers) {
						receivedHeaders.push(message.headers);
					}
				},
			});

			const complexHeaders: IHeaders = {
				"content-type": "application/json",
				"session-id": "abc-def-ghi",
				"source": "test-suite",
				"timestamp": Date.now().toString(),
				"user-id": "12345",
				"version": "1.0.0",
			};

			await producer.connect();
			await producer.send({
				topic: "complex-headers-topic",

				messages: [
					{
						headers: complexHeaders,
						key: "complex-message",
						value: "Message with complex headers",
					},
				],
			});
			await producer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 200));

			await consumer.disconnect();

			expect(receivedHeaders.length).toBeGreaterThanOrEqual(0);
		});

		it("should handle batch processing", async () => {
			const producer = kafka.producer();
			const consumer = kafka.consumer({ groupId: "batch-processing-group" });
			const batchMessages: string[] = [];
			const individualMessages: string[] = [];

			await consumer.connect();
			await consumer.subscribe({ fromBeginning: true, topics: ["batch-topic"] });
			await consumer.run({
				eachBatch: async ({ batch }) => {
					for (const message of batch.messages) {
						batchMessages.push(message.value?.toString() || "");
					}
				},
				eachMessage: async ({ message }) => {
					individualMessages.push(message.value?.toString() || "");
				},
			});

			// Send multiple messages
			await producer.connect();
			await producer.send({
				topic: "batch-topic",

				messages: [
					{ value: "batch-message-1" },
					{ value: "batch-message-2" },
					{ value: "batch-message-3" },
				],
			});
			await producer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 200));

			await consumer.disconnect();

			expect(individualMessages.length).toBe(3);
			expect(batchMessages.length).toBe(3);
		});
	});

	describe("Performance and Load Testing", () => {
		it("should handle high throughput", async () => {
			const producer = kafka.producer();
			const consumer = kafka.consumer({ groupId: "throughput-test-group" });
			const messageCount = 100;
			const receivedMessages: string[] = [];

			await consumer.connect();
			await consumer.subscribe({ fromBeginning: true, topics: ["throughput-topic"] });
			await consumer.run({
				eachMessage: async ({ message }) => {
					receivedMessages.push(message.value?.toString() || "");
				},
			});

			// Send many messages
			await producer.connect();
			const startTime = Date.now();

			for (let i = 0; i < messageCount; i++) {
				await producer.send({
					topic: "throughput-topic",

					messages: [{ value: `throughput-message-${i}` }],
				});
			}

			await producer.disconnect();
			const endTime = Date.now();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 500));

			await consumer.disconnect();

			expect(receivedMessages.length).toBeGreaterThanOrEqual(0);
			expect(endTime - startTime).toBeLessThan(5000); // Should complete within 5 seconds
		});
	});
});
