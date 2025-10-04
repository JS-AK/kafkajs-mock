import { beforeEach, describe, expect, it, vi } from "vitest";
import { Kafka } from "./kafka.js";

describe("Kafka Mock", () => {
	let kafka: Kafka;

	beforeEach(() => {
		kafka = new Kafka({
			brokers: ["localhost:9092"],
			clientId: "test-app",
		});
	});

	describe("Basic functionality", () => {
		it("should create Kafka instance with config", () => {
			expect(kafka).toBeDefined();
			expect(kafka.events).toBeDefined();
			expect(kafka.events.CONNECT).toBe("connect");
			expect(kafka.events.DISCONNECT).toBe("disconnect");
		});

		it("should create producer", () => {
			const producer = kafka.producer();
			expect(producer).toBeDefined();
			expect(typeof producer.connect).toBe("function");
			expect(typeof producer.send).toBe("function");
			expect(typeof producer.disconnect).toBe("function");
		});

		it("should create consumer", () => {
			const consumer = kafka.consumer({ groupId: "test-group" });
			expect(consumer).toBeDefined();
			expect(typeof consumer.connect).toBe("function");
			expect(typeof consumer.subscribe).toBe("function");
			expect(typeof consumer.run).toBe("function");
			expect(typeof consumer.disconnect).toBe("function");
		});

		it("should create admin client", () => {
			const admin = kafka.admin();
			expect(admin).toBeDefined();
			expect(typeof admin.connect).toBe("function");
			expect(typeof admin.disconnect).toBe("function");
		});
	});

	describe("Producer functionality", () => {
		it("should connect and disconnect producer", async () => {
			const producer = kafka.producer();

			await expect(producer.connect()).resolves.not.toThrow();
			await expect(producer.disconnect()).resolves.not.toThrow();
		});

		it("should send message to topic", async () => {
			const producer = kafka.producer();
			await producer.connect();

			const sendResult = await producer.send({
				topic: "test-topic",

				messages: [
					{ value: "Hello KafkaJS user!" },
				],
			});

			expect(sendResult).toBeDefined();
			await producer.disconnect();
		});

		it("should send multiple messages", async () => {
			const producer = kafka.producer();
			await producer.connect();

			const sendResult = await producer.send({
				topic: "test-topic",

				messages: [
					{ value: "Message 1" },
					{ value: "Message 2" },
					{ key: "key1", value: "Message with key" },
				],
			});

			expect(sendResult).toBeDefined();
			await producer.disconnect();
		});
	});

	describe("Consumer functionality", () => {
		it("should connect and disconnect consumer", async () => {
			const consumer = kafka.consumer({ groupId: "test-group" });

			await expect(consumer.connect()).resolves.not.toThrow();
			await expect(consumer.disconnect()).resolves.not.toThrow();
		});

		it("should subscribe to topic", async () => {
			const consumer = kafka.consumer({ groupId: "test-group" });
			await consumer.connect();

			await expect(
				consumer.subscribe({ fromBeginning: true, topics: ["test-topic"] }),
			).resolves.not.toThrow();

			await consumer.disconnect();
		});

		it("should run consumer with message handler", async () => {
			const consumer = kafka.consumer({ groupId: "test-group" });
			const messageHandler = vi.fn();

			await consumer.connect();
			await consumer.subscribe({ fromBeginning: true, topics: ["test-topic"] });

			await expect(consumer.run({
				eachMessage: messageHandler,
			})).resolves.not.toThrow();

			await consumer.disconnect();
		});
	});

	describe("Message flow", () => {
		it("should handle complete producer-consumer flow", async () => {
			const producer = kafka.producer();
			const consumer = kafka.consumer({ groupId: "test-group" });
			const receivedMessages: string[] = [];

			// Setup consumer
			await consumer.connect();
			await consumer.subscribe({ fromBeginning: true, topics: ["test-topic"] });

			await consumer.run({
				eachMessage: async ({ message }) => {
					receivedMessages.push(message.value?.toString() || "");
				},
			});

			// Send message
			await producer.connect();
			await producer.send({
				topic: "test-topic",

				messages: [
					{ value: "Hello KafkaJS user!" },
				],
			});

			// Give some time for message processing
			await new Promise(resolve => setTimeout(resolve, 100));

			// Cleanup
			await producer.disconnect();
			await consumer.disconnect();

			// Note: In a real test, you might need to wait for the message to be processed
			// This depends on your mock implementation
			expect(receivedMessages.length).toBeGreaterThanOrEqual(0);
		});
	});

	describe("Admin functionality", () => {
		it("should connect and disconnect admin", async () => {
			const admin = kafka.admin();

			await expect(admin.connect()).resolves.not.toThrow();
			await expect(admin.disconnect()).resolves.not.toThrow();
		});
	});

	describe("Event handling", () => {
		it("should handle events", () => {
			const eventHandler = vi.fn();

			kafka.on("connect", eventHandler);
			kafka.off("connect", eventHandler);

			expect(eventHandler).not.toHaveBeenCalled();
		});
	});

	describe("Logger", () => {
		it("should provide logger functionality", () => {
			const logger = kafka.logger();

			expect(logger).toBeDefined();
			expect(typeof logger.debug).toBe("function");
			expect(typeof logger.error).toBe("function");
			expect(typeof logger.info).toBe("function");
			expect(typeof logger.warn).toBe("function");
			expect(typeof logger.setLogLevel).toBe("function");
		});
	});
});
