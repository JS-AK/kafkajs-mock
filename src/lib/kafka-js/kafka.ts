/* eslint-disable no-console */
/* eslint-disable @typescript-eslint/no-explicit-any */
import type {
	ConsumerConfig,
	EachMessagePayload,
	Kafka as KafkaJS,
	ProducerConfig,
	ProducerRecord,
} from "kafkajs";

import { EventEmitter } from "node:events";

import { Admin } from "./admin.js";
import { Consumer } from "./consumer.js";
import { Producer } from "./producer.js";

/**
 * Mock for Kafka client
 * Emulates the behavior of a real KafkaJS client
 */
export class Kafka implements KafkaJS {
	private eventEmitter = new EventEmitter();

	public readonly events = {
		CONNECT: "connect",
		DISCONNECT: "disconnect",
		REQUEST: "request",
		REQUEST_QUEUE_SIZE: "request.queue.size",
		REQUEST_TIMEOUT: "request.timeout",
	} as const;

	public readonly logger = () => {
		const createLogger = () => ({
			namespace: createLogger,
			setLogLevel: () => { },

			debug: console.debug.bind(console),
			error: console.error.bind(console),
			info: console.info.bind(console),
			warn: console.warn.bind(console),
		});

		return createLogger();
	};

	private producers: Map<string, Producer> = new Map();
	private consumers: Map<string, Consumer> = new Map();
	private adminInstance: Admin | null = null;
	private messageStorage: Map<string, EachMessagePayload[]> = new Map();

	constructor(private config: any) {
		// Initialize mock client
	}

	/**
	 * Subscribe to event
	 */
	public on(eventName: string, listener: (...args: any[]) => void): void {
		this.eventEmitter.on(eventName, listener);
	}

	/**
	 * Unsubscribe from event
	 */
	public off(eventName: string, listener: (...args: any[]) => void): void {
		this.eventEmitter.off(eventName, listener);
	}

	/**
	 * Send message to queue and notify consumers
	 */
	public sendMessage(record: ProducerRecord): void {
		if (!this.messageStorage.has(record.topic)) {
			this.messageStorage.set(record.topic, []);
		}

		for (const message of record.messages) {
			const payload: EachMessagePayload = {
				heartbeat: async () => { },
				message: {
					attributes: 0,
					headers: message.headers ?? {},
					key: message.key ? Buffer.from(message.key) : null,
					offset: (this.messageStorage.get(record.topic)?.length ?? 0).toString(),
					timestamp: Date.now().toString(),
					value: message.value ? Buffer.from(message.value) : null,
				},
				partition: message.partition ?? 0,
				pause: () => () => { },
				topic: record.topic,
			};

			this.messageStorage.get(record.topic)?.push(payload);
			this.eventEmitter.emit("newMessage", payload);
		}
	}

	/**
	 * Get messages from queue (for testing)
	 */
	public getMessages(topics?: (string | RegExp)[]): EachMessagePayload[] {
		const allEntries = [...this.messageStorage.entries()];

		if (!topics || topics.length === 0) {
			return allEntries.flatMap(([, messages]) => messages);
		}

		return allEntries
			.filter(([topicName]) =>
				topics.some(subscribedTopic => {
					if (subscribedTopic instanceof RegExp) {
						return subscribedTopic.test(topicName);
					}
					return subscribedTopic === topicName;
				}),
			)
			.flatMap(([, messages]) => messages);
	}

	/**
	 * Clear message queue
	 */
	public clearMessages(): void {
		this.messageStorage.clear();
	}

	/**
	 * Create mock producer
	 */
	producer(config?: ProducerConfig): Producer {
		const producerId = `producer-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
		const producer = new Producer(producerId, config, this);
		this.producers.set(producerId, producer);
		return producer as unknown as Producer;
	}

	/**
	 * Create mock consumer
	 */
	consumer(config: ConsumerConfig): Consumer {
		const consumerId = `consumer-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
		const consumer = new Consumer(consumerId, config, this);
		this.consumers.set(consumerId, consumer);
		return consumer as unknown as Consumer;
	}

	/**
	 * Create mock admin client
	 */
	admin(): Admin {
		if (!this.adminInstance) {
			this.adminInstance = new Admin(this as any);
		}

		return this.adminInstance;
	}

	/**
	 * Disconnect from mock Kafka
	 */
	async disconnect(): Promise<void> {
		// Disconnect all producers
		for (const producer of this.producers.values()) {
			await producer.disconnect();
		}

		// Disconnect all consumers
		for (const consumer of this.consumers.values()) {
			await consumer.disconnect();
		}
	}
}
