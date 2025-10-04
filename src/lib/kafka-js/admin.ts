/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-explicit-any */
import type {
	AclEntry,
	AclFilter,
	Admin as AdminKafkaJS,
	DeleteAclResponse,
	DeleteGroupsResult,
	DescribeAclResponse,
	DescribeConfigResponse,
	FetchOffsetsPartition,
	GroupDescriptions,
	GroupOverview,
	IResourceConfig,
	ITopicMetadata,
	ITopicPartitionConfig,
	ListPartitionReassignmentsResponse,
	Logger,
	PartitionReassignment,
	ResourceConfigQuery,
	SeekEntry,
	TopicPartitions,
} from "kafkajs";

import type { Kafka } from "./kafka.js";

/**
 * Mock for Admin client
 */
export class Admin implements AdminKafkaJS {
	public readonly events = {
		CONNECT: "admin.connect",
		DISCONNECT: "admin.disconnect",
		REQUEST: "admin.network.request",
		REQUEST_QUEUE_SIZE: "admin.network.request_queue_size",
		REQUEST_TIMEOUT: "admin.network.request_timeout",
	} as const;

	// biome-ignore lint/suspicious/noExplicitAny: <explanation>
	[key: string]: any;

	private isConnected = false;
	private topics: Set<string> = new Set();

	constructor(private kafka: Kafka) { }
	createPartitions(options: { validateOnly?: boolean; timeout?: number; topicPartitions: ITopicPartitionConfig[] }): Promise<boolean> {
		throw new Error("Method not implemented.");
	}
	fetchTopicMetadata(options?: { topics: string[] }): Promise<{ topics: Array<ITopicMetadata> }> {
		throw new Error("Method not implemented.");
	}
	fetchOffsets(options: { groupId: string; topics?: string[]; resolveOffsets?: boolean }): Promise<Array<{ topic: string; partitions: FetchOffsetsPartition[] }>> {
		throw new Error("Method not implemented.");
	}
	fetchTopicOffsets(topic: string): Promise<Array<SeekEntry & { high: string; low: string }>> {
		throw new Error("Method not implemented.");
	}
	fetchTopicOffsetsByTimestamp(topic: string, timestamp?: number): Promise<Array<SeekEntry>> {
		throw new Error("Method not implemented.");
	}
	describeCluster(): Promise<{ brokers: Array<{ nodeId: number; host: string; port: number }>; controller: number | null; clusterId: string }> {
		throw new Error("Method not implemented.");
	}
	setOffsets(options: { groupId: string; topic: string; partitions: SeekEntry[] }): Promise<void> {
		throw new Error("Method not implemented.");
	}
	resetOffsets(options: { groupId: string; topic: string; earliest: boolean }): Promise<void> {
		throw new Error("Method not implemented.");
	}
	describeConfigs(configs: { resources: ResourceConfigQuery[]; includeSynonyms: boolean }): Promise<DescribeConfigResponse> {
		throw new Error("Method not implemented.");
	}
	alterConfigs(configs: { validateOnly: boolean; resources: IResourceConfig[] }): Promise<any> {
		throw new Error("Method not implemented.");
	}
	listGroups(): Promise<{ groups: GroupOverview[] }> {
		throw new Error("Method not implemented.");
	}
	deleteGroups(groupIds: string[]): Promise<DeleteGroupsResult[]> {
		throw new Error("Method not implemented.");
	}
	describeGroups(groupIds: string[]): Promise<GroupDescriptions> {
		throw new Error("Method not implemented.");
	}
	describeAcls(options: AclFilter): Promise<DescribeAclResponse> {
		throw new Error("Method not implemented.");
	}
	deleteAcls(options: { filters: AclFilter[] }): Promise<DeleteAclResponse> {
		throw new Error("Method not implemented.");
	}
	createAcls(options: { acl: AclEntry[] }): Promise<boolean> {
		throw new Error("Method not implemented.");
	}
	deleteTopicRecords(options: { topic: string; partitions: SeekEntry[] }): Promise<void> {
		throw new Error("Method not implemented.");
	}
	alterPartitionReassignments(request: { topics: PartitionReassignment[]; timeout?: number }): Promise<void> {
		throw new Error("Method not implemented.");
	}
	listPartitionReassignments(request: { topics?: TopicPartitions[]; timeout?: number }): Promise<ListPartitionReassignmentsResponse> {
		throw new Error("Method not implemented.");
	}
	logger(): Logger {
		throw new Error("Method not implemented.");
	}
	on(eventName: unknown, listener: unknown): import("kafkajs").RemoveInstrumentationEventListener<"admin.connect"> | import("kafkajs").RemoveInstrumentationEventListener<"admin.disconnect"> | import("kafkajs").RemoveInstrumentationEventListener<"admin.network.request"> | import("kafkajs").RemoveInstrumentationEventListener<"admin.network.request_queue_size"> | import("kafkajs").RemoveInstrumentationEventListener<"admin.network.request_timeout"> | import("kafkajs").RemoveInstrumentationEventListener<import("kafkajs").ValueOf<import("kafkajs").AdminEvents>> {
		throw new Error("Method not implemented.");
	}

	/**
	 * Connect to mock admin client
	 */
	async connect(): Promise<void> {
		this.isConnected = true;
	}

	/**
	 * Disconnect from mock admin client
	 */
	async disconnect(): Promise<void> {
		this.isConnected = false;
	}

	/**
	 * Create topics
	 */
	async createTopics(options: {
		topics: Array<{
			topic: string;
			numPartitions?: number;
			replicationFactor?: number;
		}>;
		waitForLeaders?: boolean;
		timeout?: number;
	}): Promise<boolean> {
		if (!this.isConnected) {
			throw new Error("Admin not connected");
		}

		for (const topic of options.topics) {
			this.topics.add(topic.topic);
		}

		return true;
	}

	/**
	 * Delete topics
	 */
	async deleteTopics(options: {
		topics: string[];
		timeout?: number;
	}): Promise<void> {
		if (!this.isConnected) {
			throw new Error("Admin not connected");
		}

		for (const topic of options.topics) {
			this.topics.delete(topic);
		}
		return;
	}

	/**
	 * Get list of topics
	 */
	async listTopics(): Promise<string[]> {
		if (!this.isConnected) {
			throw new Error("Admin not connected");
		}

		return Array.from(this.topics);
	}

	/**
	 * Get topic information
	 */
	async describeTopics(topics: string[]): Promise<any> {
		if (!this.isConnected) {
			throw new Error("Admin not connected");
		}

		return topics.map(topic => ({
			name: topic,
			partitions: [{
				isr: [1],
				leader: 1,
				partitionId: 0,
				replicas: [1],
			}],
		}));
	}
}
