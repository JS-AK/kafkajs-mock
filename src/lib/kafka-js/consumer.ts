/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-explicit-any */
import type {
	ConnectEvent,
	ConsumerConfig,
	ConsumerGroupJoinEvent,
	Consumer as ConsumerKafkaJS,
	ConsumerSubscribeTopics,
	DisconnectEvent,
	EachBatchPayload,
	EachMessagePayload,
	GroupDescription,
	Logger,
	TopicPartitionOffset,
	TopicPartitions,
} from "kafkajs";

import { EventEmitter } from "node:events";
import type { Kafka } from "./kafka.js";

/**
 * Mock for Consumer
 */
export class Consumer extends EventEmitter implements ConsumerKafkaJS {
	on(eventName: "consumer.connect", listener: (event: any) => void): () => void;
	on(eventName: "consumer.crash", listener: (event: any) => void): () => void;
	on(eventName: "consumer.disconnect", listener: (event: any) => void): () => void;
	on(eventName: "consumer.group_join", listener: (event: any) => void): () => void;
	on(eventName: "consumer.heartbeat", listener: (event: any) => void): () => void;
	on(eventName: "consumer.fetch_start", listener: (event: any) => void): () => void;
	on(eventName: "consumer.fetch", listener: (event: any) => void): () => void;
	on(eventName: "consumer.commit_offsets", listener: (event: any) => void): () => void;
	on(eventName: "consumer.network.request", listener: (event: any) => void): () => void;
	on(eventName: "consumer.network.request_timeout", listener: (event: any) => void): () => void;
	on(eventName: "consumer.network.request_queue_size", listener: (event: any) => void): () => void;
	on(eventName: "consumer.stop", listener: (event: any) => void): () => void;
	on(eventName: "consumer.start_batch_process", listener: (event: any) => void): () => void;
	on(eventName: "consumer.end_batch_process", listener: (event: any) => void): () => void;
	on(eventName: "consumer.rebalancing", listener: (event: any) => void): () => void;
	on(eventName: "consumer.received_unsubscribed_topics", listener: (event: any) => void): () => void;
	on(eventName: string | symbol, listener: (...args: any[]) => void): this;
	on(eventName: string | symbol, listener: (...args: any[]) => void): this | (() => void) {
		super.on(eventName, listener);
		if (typeof eventName === "string" && eventName.startsWith("consumer.")) {
			return () => this.removeListener(eventName, listener);
		}
		return this;
	}
	public readonly events = {
		COMMIT_OFFSETS: "consumer.commit_offsets",
		CONNECT: "consumer.connect",
		CRASH: "consumer.crash",
		DISCONNECT: "consumer.disconnect",
		END_BATCH_PROCESS: "consumer.end_batch_process",
		FETCH: "consumer.fetch",
		FETCH_START: "consumer.fetch_start",
		GROUP_JOIN: "consumer.group_join",
		HEARTBEAT: "consumer.heartbeat",
		REBALANCING: "consumer.rebalancing",
		RECEIVED_UNSUBSCRIBED_TOPICS: "consumer.received_unsubscribed_topics",
		REQUEST: "consumer.network.request",
		REQUEST_QUEUE_SIZE: "consumer.network.request_queue_size",
		REQUEST_TIMEOUT: "consumer.network.request_timeout",
		START_BATCH_PROCESS: "consumer.start_batch_process",
		STOP: "consumer.stop",
	} as const;

	private isConnected = false;
	private isSubscribed = false;
	private isRunning = false;
	private subscribedTopics: (string | RegExp)[] = [];
	private messageHandlers: Array<(payload: EachMessagePayload) => Promise<void>> = [];
	private batchHandlers: Array<(payload: EachBatchPayload) => Promise<void>> = [];

	constructor(
		private id: string,
		private config: ConsumerConfig,
		private kafka: Kafka,
	) {
		super();
	}
	seek(topicPartitionOffset: TopicPartitionOffset): void {
		throw new Error("Method not implemented.");
	}
	describeGroup(): Promise<GroupDescription> {
		throw new Error("Method not implemented.");
	}
	paused(): TopicPartitions[] {
		throw new Error("Method not implemented.");
	}
	logger(): Logger {
		throw new Error("Method not implemented.");
	}

	/**
	 * Connect to mock consumer
	 */
	async connect(): Promise<void> {
		this.isConnected = true;

		const payload: ConnectEvent = {
			id: "0",
			payload: null,
			timestamp: Date.now(),
			type: "consumer.connect",
		};

		this.emit(this.events.CONNECT, payload);
	}

	/**
	 * Disconnect from mock consumer
	 */
	async disconnect(): Promise<void> {
		this.isConnected = false;
		this.isRunning = false;

		const payload: DisconnectEvent = {
			id: "0",
			payload: null,
			timestamp: Date.now(),
			type: "consumer.disconnect",
		};

		this.emit(this.events.DISCONNECT, payload);
	}

	/**
	 * Subscribe to topics
	 */
	async subscribe(subscription: ConsumerSubscribeTopics): Promise<void> {
		if (!this.isConnected) {
			this.connect();
		}

		this.subscribedTopics = subscription.topics;
		this.isSubscribed = true;

		const payload: ConsumerGroupJoinEvent = {
			id: "0",
			payload: {
				duration: 0,
				groupId: this.config.groupId,
				groupProtocol: "RoundRobinAssigner",
				isLeader: false,
				leaderId: this.id,
				memberAssignment: {},
				memberId: this.id,
			},
			timestamp: Date.now(),
			type: "consumer.group_join",
		};

		this.emit(this.events.GROUP_JOIN, payload);
	}

	/**
	 * Start consumer
	 */
	async run(config: {
		eachMessage?: (payload: EachMessagePayload) => Promise<void>;
		eachBatch?: (payload: EachBatchPayload) => Promise<void>;
		autoCommit?: boolean;
		eachBatchAutoResolve?: boolean;
	}): Promise<void> {
		if (!this.isConnected || !this.isSubscribed) {
			throw new Error("Consumer not connected or not subscribed");
		}

		this.isRunning = true;

		if (config.eachMessage) {
			this.messageHandlers.push(config.eachMessage);
		}

		if (config.eachBatch) {
			this.batchHandlers.push(config.eachBatch);
		}

		// Process messages that might have been sent before the consumer was ready
		const existingMessages = this.kafka.getMessages(this.subscribedTopics);
		for (const message of existingMessages) {
			await this.handleNewMessage(message);
		}

		this.kafka.on("newMessage", this.handleNewMessage);
	}

	/**
	 * Stop consumer
	 */
	async stop(): Promise<void> {
		this.isRunning = false;
		this.messageHandlers = [];
		this.batchHandlers = [];
		this.kafka.off("newMessage", this.handleNewMessage);
	}

	/**
	 * Handle new message from Kafka mock
	 */
	private handleNewMessage = async (payload: EachMessagePayload): Promise<void> => {
		if (!this.isRunning || !this.subscribedTopics) return;

		const topicMatch = this.subscribedTopics.some(topic => {
			if (topic instanceof RegExp) {
				return topic.test(payload.topic);
			}
			return topic === payload.topic;
		});

		if (topicMatch) {
			for (const handler of this.messageHandlers) {
				await handler(payload);
			}

			if (this.batchHandlers.length > 0) {
				const batchPayload: EachBatchPayload = {
					batch: {
						firstOffset: () => payload.message.offset,
						highWatermark: (parseInt(payload.message.offset, 10) + 1).toString(),
						isEmpty: () => false,
						lastOffset: () => payload.message.offset,
						messages: [payload.message],
						offsetLag: () => "0",
						offsetLagLow: () => "0",
						partition: payload.partition,
						topic: payload.topic,
					},
					commitOffsetsIfNecessary: async () => { /* no-op */ },
					heartbeat: async () => { /* no-op */ },
					isRunning: () => this.isRunning,
					isStale: () => false,
					pause: () => () => { /* no-op */ },
					resolveOffset: () => { /* no-op */ },
					uncommittedOffsets: () => ({ topics: [] }),
				};

				for (const handler of this.batchHandlers) {
					await handler(batchPayload);
				}
			}
		}
	};

	/**
	 * Pause consumer
	 */
	pause(): void {
		// Emulate pause
	}

	/**
	 * Resume consumer
	 */
	resume(): void {
		// Emulate resume
	}

	/**
	 * Commit offsets
	 */
	async commitOffsets(_offsets: Array<{
		topic: string;
		partition: number;
		offset: string;
	}>): Promise<void> {
		// Emulate commit offsets
	}
}
