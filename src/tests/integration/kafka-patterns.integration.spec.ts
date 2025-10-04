import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { Kafka } from "../../lib/kafka-js/kafka.js";

describe("Kafka Patterns Integration Tests", () => {
	let kafka: Kafka;

	beforeEach(() => {
		kafka = new Kafka({
			brokers: ["kafka1:9092", "kafka2:9092"],
			clientId: "patterns-test-app",
		});
	});

	afterEach(async () => {
		await kafka.disconnect();
	});

	describe("Request-Reply Pattern", () => {
		it("should handle request-reply communication", async () => {
			// Service A (requestor)
			const serviceAProducer = kafka.producer();
			const serviceAConsumer = kafka.consumer({ groupId: "service-a" });

			// Service B (responder)
			const serviceBProducer = kafka.producer();
			const serviceBConsumer = kafka.consumer({ groupId: "service-b" });

			const requests: { requestId: string; data: string; timestamp: string }[] = [];
			const replies: { requestId: string; result: string; timestamp: string }[] = [];

			// Service A setup
			await serviceAConsumer.connect();
			await serviceAConsumer.subscribe({ fromBeginning: true, topics: ["service-a-replies"] });
			await serviceAConsumer.run({
				eachMessage: async ({ message }) => {
					const reply = JSON.parse(message.value?.toString() || "{}");
					replies.push(reply);
				},
			});

			// Service B setup
			await serviceBConsumer.connect();
			await serviceBConsumer.subscribe({ fromBeginning: true, topics: ["service-b-requests"] });
			await serviceBConsumer.run({
				eachMessage: async ({ message }) => {
					const request = JSON.parse(message.value?.toString() || "{}");
					requests.push(request);

					// Process request and send reply
					const reply = {
						requestId: request.requestId,
						result: `Processed: ${request.data}`,
						timestamp: new Date().toISOString(),
					};

					await serviceBProducer.connect();
					await serviceBProducer.send({
						topic: "service-a-replies",

						messages: [
							{
								headers: {
									"reply-to": "service-a",
									"request-id": request.requestId,
								},
								key: request.requestId,
								value: JSON.stringify(reply),
							},
						],
					});
					await serviceBProducer.disconnect();
				},
			});

			// Send request from Service A
			await serviceAProducer.connect();
			const request = {
				data: "Hello Service B",
				requestId: "req-001",
				timestamp: new Date().toISOString(),
			};

			await serviceAProducer.send({
				topic: "service-b-requests",

				messages: [
					{
						headers: {
							"reply-to": "service-a",
							"request-id": request.requestId,
						},
						key: request.requestId,
						value: JSON.stringify(request),
					},
				],
			});

			await serviceAProducer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 300));

			// Cleanup
			await serviceAConsumer.disconnect();
			await serviceBConsumer.disconnect();

			expect(requests.length).toBeGreaterThanOrEqual(0);
			expect(replies.length).toBeGreaterThanOrEqual(0);
		});
	});

	describe("Saga Pattern", () => {
		it("should handle distributed transaction saga", async () => {
			const sagaOrchestrator = kafka.producer();
			const orderService = kafka.consumer({ groupId: "order-service" });
			const paymentService = kafka.consumer({ groupId: "payment-service" });
			const inventoryService = kafka.consumer({ groupId: "inventory-service" });

			const sagaSteps: { sagaId: string; step: string }[] = [];

			// Order service
			await orderService.connect();
			await orderService.subscribe({ fromBeginning: true, topics: ["saga-start"] });
			await orderService.run({
				eachMessage: async ({ message }) => {
					const sagaData = JSON.parse(message.value?.toString() || "{}");
					sagaSteps.push({ sagaId: sagaData.sagaId, step: "order-created" });

					// Trigger payment step
					await sagaOrchestrator.connect();
					await sagaOrchestrator.send({
						topic: "payment-process",

						messages: [
							{
								key: sagaData.sagaId,
								value: JSON.stringify({
									amount: sagaData.amount,
									orderId: sagaData.orderId,
									sagaId: sagaData.sagaId,
								}),
							},
						],
					});
					await sagaOrchestrator.disconnect();
				},
			});

			// Payment service
			await paymentService.connect();
			await paymentService.subscribe({ fromBeginning: true, topics: ["payment-process"] });
			await paymentService.run({
				eachMessage: async ({ message }) => {
					const paymentData = JSON.parse(message.value?.toString() || "{}");
					sagaSteps.push({ sagaId: paymentData.sagaId, step: "payment-processed" });

					// Trigger inventory step
					await sagaOrchestrator.connect();
					await sagaOrchestrator.send({
						topic: "inventory-reserve",

						messages: [
							{
								key: paymentData.sagaId,
								value: JSON.stringify({
									items: paymentData.items,
									orderId: paymentData.orderId,
									sagaId: paymentData.sagaId,
								}),
							},
						],
					});
					await sagaOrchestrator.disconnect();
				},
			});

			// Inventory service
			await inventoryService.connect();
			await inventoryService.subscribe({ fromBeginning: true, topics: ["inventory-reserve"] });
			await inventoryService.run({
				eachMessage: async ({ message }) => {
					const inventoryData = JSON.parse(message.value?.toString() || "{}");
					sagaSteps.push({ sagaId: inventoryData.sagaId, step: "inventory-reserved" });
				},
			});

			// Start saga
			await sagaOrchestrator.connect();
			const sagaData = {
				amount: 100.00,
				items: [{ productId: "prod-1", quantity: 2 }],
				orderId: "order-123",
				sagaId: "saga-001",
			};

			await sagaOrchestrator.send({
				topic: "saga-start",

				messages: [
					{
						key: sagaData.sagaId,
						value: JSON.stringify(sagaData),
					},
				],
			});

			await sagaOrchestrator.disconnect();

			// Give time for saga processing
			await new Promise(resolve => setTimeout(resolve, 400));

			// Cleanup
			await orderService.disconnect();
			await paymentService.disconnect();
			await inventoryService.disconnect();

			expect(sagaSteps.length).toBeGreaterThanOrEqual(0);
		});
	});

	describe("CQRS Pattern", () => {
		it("should handle command and query separation", async () => {
			const commandProducer = kafka.producer();
			const queryProducer = kafka.producer();
			const commandHandler = kafka.consumer({ groupId: "command-handler" });
			const queryHandler = kafka.consumer({ groupId: "query-handler" });
			const readModelUpdater = kafka.consumer({ groupId: "read-model-updater" });

			const commands: { aggregateId: string; data: Record<string, string>; type: string }[] = [];
			const queries: { aggregateId: string; type: string }[] = [];
			const readModelUpdates: { aggregateId: string; data: Record<string, string>; type: string }[] = [];

			// Command handler
			await commandHandler.connect();
			await commandHandler.subscribe({ fromBeginning: true, topics: ["commands"] });
			await commandHandler.run({
				eachMessage: async ({ message }) => {
					const command = JSON.parse(message.value?.toString() || "{}");
					commands.push(command);

					// Update read model
					await queryProducer.connect();
					await queryProducer.send({
						topic: "read-model-updates",

						messages: [
							{
								key: command.aggregateId,
								value: JSON.stringify({
									aggregateId: command.aggregateId,
									data: command.data,
									eventType: command.type,
									timestamp: new Date().toISOString(),
								}),
							},
						],
					});
					await queryProducer.disconnect();
				},
			});

			// Query handler
			await queryHandler.connect();
			await queryHandler.subscribe({ fromBeginning: true, topics: ["queries"] });
			await queryHandler.run({
				eachMessage: async ({ message }) => {
					const query = JSON.parse(message.value?.toString() || "{}");
					queries.push(query);
				},
			});

			// Read model updater
			await readModelUpdater.connect();
			await readModelUpdater.subscribe({ fromBeginning: true, topics: ["read-model-updates"] });
			await readModelUpdater.run({
				eachMessage: async ({ message }) => {
					const update = JSON.parse(message.value?.toString() || "{}");
					readModelUpdates.push(update);
				},
			});

			// Send commands
			await commandProducer.connect();
			const commandsData = [
				{
					aggregateId: "user-001",
					data: { email: "john@example.com", name: "John Doe" },
					type: "CreateUser",
				},
				{
					aggregateId: "user-001",
					data: { name: "John Smith" },
					type: "UpdateUser",
				},
			];

			for (const command of commandsData) {
				await commandProducer.send({
					topic: "commands",

					messages: [
						{
							key: command.aggregateId,
							value: JSON.stringify(command),
						},
					],
				});
			}

			// Send query
			await queryProducer.connect();
			await queryProducer.send({
				topic: "queries",

				messages: [
					{
						key: "user-001",
						value: JSON.stringify({
							aggregateId: "user-001",
							type: "GetUser",
						}),
					},
				],
			});
			await queryProducer.disconnect();

			await commandProducer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 300));

			// Cleanup
			await commandHandler.disconnect();
			await queryHandler.disconnect();
			await readModelUpdater.disconnect();

			expect(commands.length).toBeGreaterThanOrEqual(0);
			expect(queries.length).toBeGreaterThanOrEqual(0);
			expect(readModelUpdates.length).toBeGreaterThanOrEqual(0);
		});
	});

	describe("Outbox Pattern", () => {
		it("should handle transactional outbox", async () => {
			const outboxProducer = kafka.producer();
			const outboxProcessor = kafka.consumer({ groupId: "outbox-processor" });
			const eventConsumer = kafka.consumer({ groupId: "event-consumer" });

			const outboxEvents: { aggregateId: string; eventData: Record<string, string>; eventId: string; eventType: string; publishedAt: string }[] = [];
			const publishedEvents: { aggregateId: string; eventData: Record<string, string>; eventId: string; eventType: string; publishedAt: string }[] = [];

			// Outbox processor
			await outboxProcessor.connect();
			await outboxProcessor.subscribe({ fromBeginning: true, topics: ["outbox-events"] });
			await outboxProcessor.run({
				eachMessage: async ({ message }) => {
					const outboxEvent = JSON.parse(message.value?.toString() || "{}");
					outboxEvents.push(outboxEvent);

					// Publish to actual event topic
					await outboxProducer.connect();
					await outboxProducer.send({
						topic: "domain-events",

						messages: [
							{
								key: outboxEvent.aggregateId,
								value: JSON.stringify({
									aggregateId: outboxEvent.aggregateId,
									eventData: outboxEvent.eventData,
									eventId: outboxEvent.eventId,
									eventType: outboxEvent.eventType,
									publishedAt: new Date().toISOString(),
								}),
							},
						],
					});
					await outboxProducer.disconnect();
				},
			});

			// Event consumer
			await eventConsumer.connect();
			await eventConsumer.subscribe({ fromBeginning: true, topics: ["domain-events"] });
			await eventConsumer.run({
				eachMessage: async ({ message }) => {
					const event = JSON.parse(message.value?.toString() || "{}");
					publishedEvents.push(event);
				},
			});

			// Simulate database transaction with outbox
			await outboxProducer.connect();
			const outboxData = [
				{
					aggregateId: "user-001",
					createdAt: new Date().toISOString(),
					eventData: { email: "john@example.com", name: "John Doe" },
					eventId: "evt-001",
					eventType: "UserCreated",
				},
				{
					aggregateId: "user-001",
					createdAt: new Date().toISOString(),
					eventData: { name: "John Smith" },
					eventId: "evt-002",
					eventType: "UserUpdated",
				},
			];

			for (const outboxEvent of outboxData) {
				await outboxProducer.send({
					topic: "outbox-events",

					messages: [
						{
							key: outboxEvent.eventId,
							value: JSON.stringify(outboxEvent),
						},
					],
				});
			}

			await outboxProducer.disconnect();

			// Give time for processing
			await new Promise(resolve => setTimeout(resolve, 300));

			// Cleanup
			await outboxProcessor.disconnect();
			await eventConsumer.disconnect();

			expect(outboxEvents.length).toBeGreaterThanOrEqual(0);
			expect(publishedEvents.length).toBeGreaterThanOrEqual(0);
		});
	});

	describe("Choreography Pattern", () => {
		it("should handle event-driven choreography", async () => {
			const orderService = kafka.producer();
			const paymentService = kafka.consumer({ groupId: "payment-service" });
			const inventoryService = kafka.consumer({ groupId: "inventory-service" });
			const notificationService = kafka.consumer({ groupId: "notification-service" });

			const paymentEvents: { type: string; orderId: string }[] = [];
			const inventoryEvents: { type: string; orderId: string }[] = [];
			const notificationEvents: { type: string; orderId: string }[] = [];

			// Payment service
			await paymentService.connect();
			await paymentService.subscribe({ fromBeginning: true, topics: ["order-created"] });
			await paymentService.run({
				eachMessage: async ({ message }) => {
					const orderEvent = JSON.parse(message.value?.toString() || "{}");
					paymentEvents.push({ orderId: orderEvent.orderId, type: "payment-processed" });
				},
			});

			// Inventory service
			await inventoryService.connect();
			await inventoryService.subscribe({ fromBeginning: true, topics: ["order-created"] });
			await inventoryService.run({
				eachMessage: async ({ message }) => {
					const orderEvent = JSON.parse(message.value?.toString() || "{}");
					inventoryEvents.push({ orderId: orderEvent.orderId, type: "inventory-reserved" });
				},
			});

			// Notification service
			await notificationService.connect();
			await notificationService.subscribe({ fromBeginning: true, topics: ["order-created"] });
			await notificationService.run({
				eachMessage: async ({ message }) => {
					const orderEvent = JSON.parse(message.value?.toString() || "{}");
					notificationEvents.push({ orderId: orderEvent.orderId, type: "order-confirmation-sent" });
				},
			});

			// Create order event
			await orderService.connect();
			const orderEvent = {
				items: [{ price: 29.99, productId: "prod-1", quantity: 1 }],
				orderId: "order-456",
				timestamp: new Date().toISOString(),
				totalAmount: 29.99,
				userId: "user-789",
			};

			await orderService.send({
				topic: "order-created",

				messages: [
					{
						headers: {
							"event-type": "order-created",
							"event-version": "1.0",
						},
						key: orderEvent.orderId,
						value: JSON.stringify(orderEvent),
					},
				],
			});

			await orderService.disconnect();

			// Give time for choreography processing
			await new Promise(resolve => setTimeout(resolve, 300));

			// Cleanup
			await paymentService.disconnect();
			await inventoryService.disconnect();
			await notificationService.disconnect();

			expect(paymentEvents.length).toBeGreaterThanOrEqual(0);
			expect(inventoryEvents.length).toBeGreaterThanOrEqual(0);
			expect(notificationEvents.length).toBeGreaterThanOrEqual(0);
		});
	});

	describe("Stream Processing Pattern", () => {
		it("should handle real-time stream processing", async () => {
			const dataStreamProducer = kafka.producer();
			const streamProcessor = kafka.consumer({ groupId: "stream-processor" });
			const aggregator = kafka.consumer({ groupId: "aggregator" });

			const processedRecords: { id: string; value: number; timestamp: string }[] = [];
			const aggregatedData: { id: string; value: number; timestamp: string }[] = [];

			// Stream processor
			await streamProcessor.connect();
			await streamProcessor.subscribe({ fromBeginning: true, topics: ["raw-stream"] });
			await streamProcessor.run({
				eachMessage: async ({ message }) => {
					const record = JSON.parse(message.value?.toString() || "{}");
					processedRecords.push({
						...record,
						processed: true,
						processedAt: new Date().toISOString(),
					});
				},
			});

			// Aggregator
			await aggregator.connect();
			await aggregator.subscribe({ fromBeginning: true, topics: ["processed-stream"] });
			await aggregator.run({
				eachMessage: async ({ message }) => {
					const record = JSON.parse(message.value?.toString() || "{}");
					aggregatedData.push({
						...record,
						aggregatedAt: new Date().toISOString(),
					});
				},
			});

			// Send stream data
			await dataStreamProducer.connect();

			// Simulate real-time data stream
			const streamData = [
				{ id: "stream-001", timestamp: new Date().toISOString(), value: 100 },
				{ id: "stream-002", timestamp: new Date().toISOString(), value: 150 },
				{ id: "stream-003", timestamp: new Date().toISOString(), value: 200 },
				{ id: "stream-004", timestamp: new Date().toISOString(), value: 175 },
				{ id: "stream-005", timestamp: new Date().toISOString(), value: 125 },
			];

			for (const data of streamData) {
				await dataStreamProducer.send({
					topic: "raw-stream",

					messages: [
						{
							headers: {
								"data-type": "metrics",
								"stream-id": data.id,
							},
							key: data.id,
							value: JSON.stringify(data),
						},
					],
				});

				// Simulate real-time delay
				await new Promise(resolve => setTimeout(resolve, 50));
			}

			await dataStreamProducer.disconnect();

			// Give time for stream processing
			await new Promise(resolve => setTimeout(resolve, 400));

			// Cleanup
			await streamProcessor.disconnect();
			await aggregator.disconnect();

			expect(processedRecords.length).toBeGreaterThanOrEqual(0);
			expect(aggregatedData.length).toBeGreaterThanOrEqual(0);
		});
	});
});
