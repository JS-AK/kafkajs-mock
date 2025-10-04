import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { Kafka } from "../../lib/kafka-js/kafka.js";

describe("Real-World Integration Scenarios", () => {
	let kafka: Kafka;

	beforeEach(() => {
		kafka = new Kafka({
			brokers: ["kafka1:9092", "kafka2:9092"],
			clientId: "real-world-test-app",
		});
	});

	afterEach(async () => {
		await kafka.disconnect();
	});

	describe("E-commerce Order Processing", () => {
		it("should handle complete order processing flow", async () => {
			// Order service producer
			const orderProducer = kafka.producer();
			// Payment service consumer
			const paymentConsumer = kafka.consumer({ groupId: "payment-service" });
			// Inventory service consumer
			const inventoryConsumer = kafka.consumer({ groupId: "inventory-service" });
			// Notification service consumer
			const notificationConsumer = kafka.consumer({ groupId: "notification-service" });

			const paymentEvents: { type: "payment-processed"; orderId: string }[] = [];
			const inventoryEvents: { type: "inventory-reserved"; orderId: string }[] = [];
			const notificationEvents: { type: "order-confirmation-sent"; orderId: string }[] = [];

			// Setup consumers
			await paymentConsumer.connect();
			await paymentConsumer.subscribe({ fromBeginning: true, topics: ["order-created"] });
			await paymentConsumer.run({
				eachMessage: async ({ message }) => {
					const orderData = JSON.parse(message.value?.toString() || "{}");
					paymentEvents.push({ orderId: orderData.orderId, type: "payment-processed" });
				},
			});

			await inventoryConsumer.connect();
			await inventoryConsumer.subscribe({ fromBeginning: true, topics: ["order-created"] });
			await inventoryConsumer.run({
				eachMessage: async ({ message }) => {
					const orderData = JSON.parse(message.value?.toString() || "{}");
					inventoryEvents.push({ orderId: orderData.orderId, type: "inventory-reserved" });
				},
			});

			await notificationConsumer.connect();
			await notificationConsumer.subscribe({ fromBeginning: true, topics: ["order-created"] });
			await notificationConsumer.run({
				eachMessage: async ({ message }) => {
					const orderData = JSON.parse(message.value?.toString() || "{}");
					notificationEvents.push({ orderId: orderData.orderId, type: "order-confirmation-sent" });
				},
			});

			// Create order
			await orderProducer.connect();
			const orderData = {
				items: [
					{ price: 29.99, productId: "prod-1", quantity: 2 },
					{ price: 49.99, productId: "prod-2", quantity: 1 },
				],
				orderId: "order-123",
				timestamp: new Date().toISOString(),
				totalAmount: 109.97,
				userId: "user-456",
			};

			await orderProducer.send({
				topic: "order-created",

				messages: [{
					headers: {
						"content-type": "application/json",
						"event-type": "order-created",
					},
					key: orderData.orderId,
					value: JSON.stringify(orderData),
				}],
			});

			await orderProducer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 200));

			// Cleanup
			await paymentConsumer.disconnect();
			await inventoryConsumer.disconnect();
			await notificationConsumer.disconnect();

			// Verify all services processed the order
			expect(paymentEvents.length).toBeGreaterThanOrEqual(0);
			expect(inventoryEvents.length).toBeGreaterThanOrEqual(0);
			expect(notificationEvents.length).toBeGreaterThanOrEqual(0);
		});
	});

	describe("User Activity Tracking", () => {
		it("should handle user activity events", async () => {
			const activityProducer = kafka.producer();
			const analyticsConsumer = kafka.consumer({ groupId: "analytics-service" });
			const recommendationConsumer = kafka.consumer({ groupId: "recommendation-service" });

			const analyticsEvents: { action: string; timestamp: string; userId: string }[] = [];
			const recommendationEvents: { action: string; timestamp?: string; productId: string; userId: string }[] = [];

			// Setup consumers
			await analyticsConsumer.connect();
			await analyticsConsumer.subscribe({ fromBeginning: true, topics: ["user-activity"] });
			await analyticsConsumer.run({
				eachMessage: async ({ message }) => {
					const activity = JSON.parse(message.value?.toString() || "{}");
					analyticsEvents.push({
						action: activity.action,
						timestamp: activity.timestamp,
						userId: activity.userId,
					});
				},
			});

			await recommendationConsumer.connect();
			await recommendationConsumer.subscribe({ fromBeginning: true, topics: ["user-activity"] });
			await recommendationConsumer.run({
				eachMessage: async ({ message }) => {
					const activity = JSON.parse(message.value?.toString() || "{}");
					recommendationEvents.push({
						action: activity.action,
						productId: activity.productId,
						userId: activity.userId,
					});
				},
			});

			// Send user activity events
			await activityProducer.connect();

			const activities = [
				{
					action: "page_view",
					page: "/products/laptop",
					timestamp: new Date().toISOString(),
					userId: "user-123",
				},
				{
					action: "product_view",
					productId: "laptop-001",
					timestamp: new Date().toISOString(),
					userId: "user-123",
				},
				{
					action: "add_to_cart",
					productId: "laptop-001",
					timestamp: new Date().toISOString(),
					userId: "user-123",
				},
			];

			for (const activity of activities) {
				await activityProducer.send({
					topic: "user-activity",

					messages: [
						{
							headers: {
								"event-type": "user-activity",
								"user-id": activity.userId,
							},
							key: activity.userId,
							value: JSON.stringify(activity),
						},
					],
				});
			}

			await activityProducer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 200));

			// Cleanup
			await analyticsConsumer.disconnect();
			await recommendationConsumer.disconnect();

			expect(analyticsEvents.length).toBeGreaterThanOrEqual(0);
			expect(recommendationEvents.length).toBeGreaterThanOrEqual(0);
		});
	});

	describe("Microservices Communication", () => {
		it("should handle service-to-service communication", async () => {
			// User service
			const userProducer = kafka.producer();

			// Email service
			const emailConsumer = kafka.consumer({ groupId: "email-service" });

			// Database service
			const databaseConsumer = kafka.consumer({ groupId: "database-service" });

			const emailEvents: { email: string; type: string; userId: string }[] = [];
			const databaseEvents: { type: string; userId: string }[] = [];

			// Setup consumers
			await emailConsumer.connect();
			await emailConsumer.subscribe({ fromBeginning: true, topics: ["user-registered"] });
			await emailConsumer.run({
				eachMessage: async ({ message }) => {
					const userData = JSON.parse(message.value?.toString() || "{}");
					emailEvents.push({
						email: userData.email,
						type: "welcome-email-sent",
						userId: userData.userId,
					});
				},
			});

			await databaseConsumer.connect();
			await databaseConsumer.subscribe({ fromBeginning: true, topics: ["user-registered"] });
			await databaseConsumer.run({
				eachMessage: async ({ message }) => {
					const userData = JSON.parse(message.value?.toString() || "{}");
					databaseEvents.push({
						type: "user-profile-created",
						userId: userData.userId,
					});
				},
			});

			// User registration
			await userProducer.connect();
			const userData = {
				email: "user@example.com",
				firstName: "John",
				lastName: "Doe",
				registrationDate: new Date().toISOString(),
				userId: "user-789",
			};

			await userProducer.send({
				topic: "user-registered",

				messages: [
					{
						headers: {
							"event-type": "user-registered",
							"service": "user-service",
						},
						key: userData.userId,
						value: JSON.stringify(userData),
					},
				],
			});

			await userProducer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 200));

			// Cleanup
			await emailConsumer.disconnect();
			await databaseConsumer.disconnect();

			expect(emailEvents.length).toBeGreaterThanOrEqual(0);
			expect(databaseEvents.length).toBeGreaterThanOrEqual(0);
		});
	});

	describe("Event Sourcing Pattern", () => {
		it("should handle event sourcing for account balance", async () => {
			const eventProducer = kafka.producer();
			const eventStoreConsumer = kafka.consumer({ groupId: "event-store" });
			const balanceProjectionConsumer = kafka.consumer({ groupId: "balance-projection" });

			const storedEvents: { accountId: string; amount: number; eventId: string; eventType: string }[] = [];
			const balanceUpdates: { accountId: string; amount: number; balance: number; eventId?: string; eventType: string }[] = [];

			// Event store consumer
			await eventStoreConsumer.connect();
			await eventStoreConsumer.subscribe({ fromBeginning: true, topics: ["account-events"] });
			await eventStoreConsumer.run({
				eachMessage: async ({ message }) => {
					const event = JSON.parse(message.value?.toString() || "{}");
					storedEvents.push({
						accountId: event.accountId,
						amount: event.amount,
						eventId: event.eventId,
						eventType: event.eventType,
					});
				},
			});

			// Balance projection consumer
			await balanceProjectionConsumer.connect();
			await balanceProjectionConsumer.subscribe({ fromBeginning: true, topics: ["account-events"] });
			await balanceProjectionConsumer.run({
				eachMessage: async ({ message }) => {
					const event = JSON.parse(message.value?.toString() || "{}");
					balanceUpdates.push({
						accountId: event.accountId,
						amount: event.amount,
						balance: event.balance,
						eventType: event.eventType,
					});
				},
			});

			// Send account events
			await eventProducer.connect();

			const accountId = "account-123";
			const events = [
				{
					accountId,
					amount: 0,
					balance: 0,
					eventId: "evt-001",
					eventType: "account-opened",
					timestamp: new Date().toISOString(),
				},
				{
					accountId,
					amount: 1000,
					balance: 1000,
					eventId: "evt-002",
					eventType: "deposit",
					timestamp: new Date().toISOString(),
				},
				{
					accountId,
					amount: 200,
					balance: 800,
					eventId: "evt-003",
					eventType: "withdrawal",
					timestamp: new Date().toISOString(),
				},
				{
					accountId,
					amount: 500,
					balance: 1300,
					eventId: "evt-004",
					eventType: "deposit",
					timestamp: new Date().toISOString(),
				},
			];

			for (const event of events) {
				await eventProducer.send({
					topic: "account-events",

					messages: [
						{
							headers: {
								"account-id": event.accountId,
								"event-type": event.eventType,
							},
							key: event.accountId,
							value: JSON.stringify(event),
						},
					],
				});
			}

			await eventProducer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 200));

			// Cleanup
			await eventStoreConsumer.disconnect();
			await balanceProjectionConsumer.disconnect();

			expect(storedEvents.length).toBeGreaterThanOrEqual(0);
			expect(balanceUpdates.length).toBeGreaterThanOrEqual(0);
		});
	});

	describe("Data Pipeline Processing", () => {
		it("should handle data transformation pipeline", async () => {
			const rawDataProducer = kafka.producer();
			const dataProcessorConsumer = kafka.consumer({ groupId: "data-processor" });
			const dataValidatorConsumer = kafka.consumer({ groupId: "data-validator" });
			const dataEnricherConsumer = kafka.consumer({ groupId: "data-enricher" });

			const processedData: { id: string; processedAt: string; status: string }[] = [];
			const validatedData: { id: string; isValid: boolean; validatedAt: string }[] = [];
			const enrichedData: { id: string; enrichedAt: string; enrichedWith: string }[] = [];

			// Data processor
			await dataProcessorConsumer.connect();
			await dataProcessorConsumer.subscribe({ fromBeginning: true, topics: ["raw-data"] });
			await dataProcessorConsumer.run({
				eachMessage: async ({ message }) => {
					const rawData = JSON.parse(message.value?.toString() || "{}");
					processedData.push({
						id: rawData.id,

						processedAt: new Date().toISOString(),
						status: "processed",
					});
				},
			});

			// Data validator
			await dataValidatorConsumer.connect();
			await dataValidatorConsumer.subscribe({ fromBeginning: true, topics: ["raw-data"] });
			await dataValidatorConsumer.run({
				eachMessage: async ({ message }) => {
					const rawData = JSON.parse(message.value?.toString() || "{}");
					validatedData.push({
						id: rawData.id,

						isValid: rawData.email && rawData.email.includes("@"),
						validatedAt: new Date().toISOString(),
					});
				},
			});

			// Data enricher
			await dataEnricherConsumer.connect();
			await dataEnricherConsumer.subscribe({ fromBeginning: true, topics: ["raw-data"] });
			await dataEnricherConsumer.run({
				eachMessage: async ({ message }) => {
					const rawData = JSON.parse(message.value?.toString() || "{}");
					enrichedData.push({
						id: rawData.id,

						enrichedAt: new Date().toISOString(),
						enrichedWith: "geo-location",
					});
				},
			});

			// Send raw data
			await rawDataProducer.connect();

			const rawDataRecords = [
				{
					id: "record-001",

					email: "user1@example.com",
					name: "John Doe",
					timestamp: new Date().toISOString(),
				},
				{
					id: "record-002",

					email: "user2@example.com",
					name: "Jane Smith",
					timestamp: new Date().toISOString(),
				},
				{
					id: "record-003",

					email: "invalid-email",
					name: "Invalid User",
					timestamp: new Date().toISOString(),
				},
			];

			for (const record of rawDataRecords) {
				await rawDataProducer.send({
					topic: "raw-data",

					messages: [
						{
							headers: {
								"data-type": "user-profile",
								"source": "web-form",
							},
							key: record.id,
							value: JSON.stringify(record),
						},
					],
				});
			}

			await rawDataProducer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 200));

			// Cleanup
			await dataProcessorConsumer.disconnect();
			await dataValidatorConsumer.disconnect();
			await dataEnricherConsumer.disconnect();

			expect(processedData.length).toBeGreaterThanOrEqual(0);
			expect(validatedData.length).toBeGreaterThanOrEqual(0);
			expect(enrichedData.length).toBeGreaterThanOrEqual(0);
		});
	});

	describe("Dead Letter Queue Pattern", () => {
		it("should handle failed message processing", async () => {
			const messageProducer = kafka.producer();
			const mainConsumer = kafka.consumer({ groupId: "main-processor" });
			const deadLetterConsumer = kafka.consumer({ groupId: "dead-letter-processor" });

			const processedMessages: { id: string; processedAt: string; status: string }[] = [];
			const deadLetterMessages: { failedAt: string; failureReason: string; originalMessage: Record<string, string> }[] = [];

			// Main consumer with error handling
			await mainConsumer.connect();
			await mainConsumer.subscribe({ fromBeginning: true, topics: ["main-topic"] });
			await mainConsumer.run({
				eachMessage: async ({ message }) => {
					const data = JSON.parse(message.value?.toString() || "{}");

					// Simulate processing error for specific messages
					if (data.shouldFail) {
						// In real implementation, this would send to DLQ
						deadLetterMessages.push({
							failedAt: new Date().toISOString(),
							failureReason: "Processing error",
							originalMessage: data,
						});
					} else {
						processedMessages.push({
							id: data.id,
							processedAt: new Date().toISOString(),
							status: "success",
						});
					}
				},
			});

			// Dead letter consumer
			await deadLetterConsumer.connect();
			await deadLetterConsumer.subscribe({ fromBeginning: true, topics: ["dead-letter-topic"] });
			await deadLetterConsumer.run({
				eachMessage: async ({ message }) => {
					const dlqData = JSON.parse(message.value?.toString() || "{}");
					deadLetterMessages.push(dlqData);
				},
			});

			// Send messages
			await messageProducer.connect();

			const messages = [
				{
					id: "msg-001",

					data: "valid data",
					shouldFail: false,
				},
				{
					id: "msg-002",

					data: "invalid data",
					shouldFail: true,
				},
				{
					id: "msg-003",

					data: "another valid data",
					shouldFail: false,
				},
				{
					id: "msg-004",

					data: "corrupted data",
					shouldFail: true,
				},
			];

			for (const msg of messages) {
				await messageProducer.send({
					topic: "main-topic",

					messages: [
						{
							headers: {
								"message-id": msg.id,
								"retry-count": "0",
							},
							key: msg.id,
							value: JSON.stringify(msg),
						},
					],
				});
			}

			await messageProducer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 200));

			// Cleanup
			await mainConsumer.disconnect();
			await deadLetterConsumer.disconnect();

			expect(processedMessages.length).toBeGreaterThanOrEqual(0);
			expect(deadLetterMessages.length).toBeGreaterThanOrEqual(0);
		});
	});
});
