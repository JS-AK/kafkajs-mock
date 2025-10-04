
/* eslint-disable @typescript-eslint/no-explicit-any */
import type {
	ConnectEvent,
	DisconnectEvent,
	Logger,
	ProducerBatch,
	ProducerConfig,
	Producer as ProducerKafkaJS,
	ProducerRecord,
	RecordMetadata,
	Transaction,
} from "kafkajs";

import { EventEmitter } from "node:events";

import type { Kafka } from "./kafka.js";

/**
 * Mock for Producer
 */
export class Producer extends EventEmitter implements ProducerKafkaJS {
	on(eventName: "producer.connect", listener: (event: any) => void): () => void;
	on(eventName: "producer.disconnect", listener: (event: any) => void): () => void;
	on(eventName: "producer.network.request", listener: (event: any) => void): () => void;
	on(eventName: "producer.network.request_queue_size", listener: (event: any) => void): () => void;
	on(eventName: "producer.network.request_timeout", listener: (event: any) => void): () => void;
	on(eventName: string, listener: (...args: any[]) => void): this;
	on(eventName: string, listener: (...args: any[]) => void): this | (() => void) {
		super.on(eventName, listener);
		if (eventName.startsWith("producer.")) {
			return () => this.removeListener(eventName, listener);
		}
		return this;
	}

	public readonly events = {
		CONNECT: "producer.connect",
		DISCONNECT: "producer.disconnect",
		REQUEST: "producer.network.request",
		REQUEST_QUEUE_SIZE: "producer.network.request_queue_size",
		REQUEST_TIMEOUT: "producer.network.request_timeout",
	} as const;

	private isConnected = false;

	constructor(
		private id: string,
		private config: ProducerConfig | undefined,
		private kafka: Kafka,
	) {
		super();
	}

	isIdempotent(): boolean {
		throw new Error("Method not implemented.");
	}

	transaction(): Promise<Transaction> {
		throw new Error("Method not implemented.");
	}

	logger(): Logger {
		throw new Error("Method not implemented.");
	}

	/**
	 * Connect to mock producer
	 */
	async connect(): Promise<void> {
		this.isConnected = true;

		const payload: ConnectEvent = {
			id: "0",
			payload: null,
			timestamp: Date.now(),
			type: "producer.connect",
		};

		this.emit(this.events.CONNECT, payload);
	}

	/**
	 * Disconnect from mock producer
	 */
	async disconnect(): Promise<void> {
		this.isConnected = false;

		const payload: DisconnectEvent = {
			id: "0",
			payload: null,
			timestamp: Date.now(),
			type: "producer.disconnect",
		};

		this.emit(this.events.DISCONNECT, payload);
	}

	/**
	 * Send messages to mock topic
	 */
	async send(record: ProducerRecord): Promise<RecordMetadata[]> {
		if (!this.isConnected) {
			throw new Error("Producer not connected");
		}

		this.kafka.sendMessage(record);

		// Emulate network delay
		await new Promise(resolve => setTimeout(resolve, 10));

		return record.messages.map((message, index) => ({
			errorCode: 0,
			logAppendTime: Date.now().toString(),
			logStartOffset: "0",
			offset: index.toString(),
			partition: message.partition ?? 0,
			timestamp: Date.now().toString(),
			topicName: record.topic,
		}));
	}

	/**
	 * Send batch messages
	 */
	async sendBatch(batch: ProducerBatch): Promise<RecordMetadata[]> {
		const results: RecordMetadata[] = [];
		for (const record of batch.topicMessages || []) {
			const metadata = await this.send(record);
			results.push(...metadata);
		}
		return results;
	}
}
