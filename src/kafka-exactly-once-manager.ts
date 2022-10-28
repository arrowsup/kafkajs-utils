import {
  Consumer,
  ConsumerConfig,
  Kafka,
  KafkaConfig,
  Logger,
  Producer,
  ProducerConfig,
} from "kafkajs";

export type KafkaExactlyOnceManagerConfig = {
  transactionIdPrefix: string;
  kafkaConfig: KafkaConfig;
  createConsumerConfig: Omit<ConsumerConfig, "readUncommitted">;
  createProducerOptions: Omit<
    ProducerConfig,
    "idempotent" | "maxInFlightRequests" | "transactionalId"
  >;
};

export class KafkaExactlyOnceManager {
  /** Map of transactionalId -> Producer. */
  private readonly producerMap: Map<string, Producer> = new Map();
  private consumer: Consumer | undefined = undefined;
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
    return `${this.config.transactionIdPrefix}-${sourceTopic}-${sourcePartition}`;
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
   * Remove all references to producers and consumers and disconnect them.
   */
  readonly cleanUp = async (): Promise<void> => {
    // Stop consuming, then stop producing.
    await this.cleanUpConsumers();
    await this.cleanUpProducers();
  };

  private readonly onRebalance = async () => {
    // Remove existing producers, so they can be recreated for
    // their new topics / partitions.
    this.logger.info(this.logPrefix + "starting rebalance cleanup");
    await this.cleanUpProducers();
    this.logger.info(this.logPrefix + "finished rebalance cleanup");
  };

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

  readonly getExactlyOnceCompatibleProducer = async (
    sourceTopic: string,
    sourcePartition: number
  ): Promise<Producer> => {
    const transactionalId = this.getTransactionalId(
      sourceTopic,
      sourcePartition
    );

    const existingProducer = this.producerMap.get(transactionalId);

    if (existingProducer) {
      // Already have a producer for this topic & read partition.
      return existingProducer;
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
      return newProducer;
    }
  };

  readonly getConsumerGroupId = () => {
    return this.config.createConsumerConfig.groupId;
  };
}
