import {
  Consumer,
  ConsumerConfig,
  Kafka,
  KafkaConfig,
  Logger,
  Producer,
  ProducerConfig,
  Transaction,
} from "kafkajs";

export type KafkaExactlyOnceManagerConfig = {
  transactionalIdPrefix: string;
  kafkaConfig: KafkaConfig;
  createConsumerConfig: Omit<ConsumerConfig, "readUncommitted">;
  createProducerOptions: Omit<
    ProducerConfig,
    "idempotent" | "maxInFlightRequests" | "transactionalId"
  >;
};

/**
 * Manages a consumer and set of producers that are configured for
 * Exactly Once Semantics (EOS) for the given transactionalIdPrefix.
 *
 * TransactionIds are determined by source topic and partition.
 * See [Choosing a transactionalId](https://kafka.js.org/docs/transactions#choosing-a-transactionalid)
 * for more information.
 *
 * This manager assumes a single source topic.
 */
export class KafkaExactlyOnceManager {
  private consumer: Consumer | undefined = undefined;

  /** Map of transactionalId -> Producer. */
  private readonly producerMap: Map<string, Producer> = new Map();
  private readonly kafka: Kafka;
  private readonly logger: Logger;

  constructor(private readonly config: KafkaExactlyOnceManagerConfig) {
    this.kafka = new Kafka(config.kafkaConfig);
    this.logger = this.kafka.logger();
  }

  private readonly logPrefix = KafkaExactlyOnceManager.name + ": ";

  private readonly getTransactionalId = (
    sourceTopic: string,
    sourcePartition: number
  ): string => {
    return `${this.config.transactionalIdPrefix}-${sourceTopic}-${sourcePartition}`;
  };

  /**
   * Clear producer map and disconnect any producers.
   */
  private readonly cleanUpProducers = async () => {
    // "Atomically" (in the sense of the JS event loop) clear the map,
    // while remembering the old producers to disconnect below.
    const oldProducers = Array.from(this.producerMap.values());
    this.producerMap.clear();

    // Then disconnect each old producer.
    for (const producer of oldProducers) {
      await producer.disconnect();
    }
  };

  /**
   * Clear consumer and disconnect it, if it exists.
   */
  private readonly cleanUpConsumers = async () => {
    if (this.consumer) {
      // "Atomically" (in the sense of the JS event loop) clear the consumer,
      // then disconnect it.
      const consumer = this.consumer;
      this.consumer = undefined;
      await consumer.disconnect();
    }
  };

  /**
   * Remove all references to any allocated producers and consumers and disconnect them.
   */
  readonly cleanUp = async (): Promise<void> => {
    // Stop consuming, then stop producing.
    await this.cleanUpConsumers();
    await this.cleanUpProducers();
  };

  private readonly onRebalance = async () => {
    // Remove existing producers, so they can be recreated for
    // the new set of source topics + partitions our consumer is assigned.
    this.logger.info(this.logPrefix + "starting rebalance cleanup");
    await this.cleanUpProducers();
    this.logger.info(this.logPrefix + "finished rebalance cleanup");
  };

  /**
   * Get the EOS compatible consumer.
   *
   * @returns EOS compatible consumer.
   */
  readonly getExactlyOnceCompatibleConsumer = async (): Promise<Consumer> => {
    if (this.consumer) {
      return this.consumer;
    } else {
      this.logger.info(this.logPrefix + "allocating consumer");
      this.consumer = this.kafka.consumer({
        ...this.config.createConsumerConfig,
        readUncommitted: false,
      });

      await this.consumer.connect();

      this.consumer.on("consumer.rebalancing", () => void this.onRebalance());

      this.logger.info(this.logPrefix + "allocated consumer");
      return this.consumer;
    }
  };

  /**
   * Returns a transaction allocated from a producer that has
   * EOS configured for the given parameters.
   *
   * @param sourceTopic Source Topic data will be derived from.
   * @param sourcePartition Source Partition data will be derived from.
   * @returns Transaction object configured for the given parameters.
   */
  readonly getExactlyOnceCompatibleTransaction = async (
    sourceTopic: string,
    sourcePartition: number
  ): Promise<Transaction> => {
    const transactionalId = this.getTransactionalId(
      sourceTopic,
      sourcePartition
    );

    const existingProducer = this.producerMap.get(transactionalId);

    if (existingProducer) {
      // Already have a producer for this topic & read partition.
      return existingProducer.transaction();
    } else {
      // No producer yet -> make one.
      this.logger.info(this.logPrefix + "allocating producer");

      const newProducer = this.kafka.producer({
        ...this.config.createProducerOptions,
        idempotent: true,
        maxInFlightRequests: 1,
        transactionalId,
      });

      await newProducer.connect();

      this.producerMap.set(transactionalId, newProducer);

      this.logger.info(this.logPrefix + "allocated producer");
      return newProducer.transaction();
    }
  };

  /**
   * @returns Consumer Group ID this manager was configured with.
   */
  readonly getConsumerGroupId = () => {
    return this.config.createConsumerConfig.groupId;
  };
}