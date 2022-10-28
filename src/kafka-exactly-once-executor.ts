import {
  CompressionTypes,
  Consumer,
  EachMessagePayload,
  Message,
} from "kafkajs";
import {
  KafkaExactlyOnceManager,
  KafkaExactlyOnceManagerConfig,
} from "./kafka-exactly-once-manager";

type KafkaExactlyOnceExecutorConfig = {
  processor: (event: EachMessagePayload) => Promise<Message>;
  subscribeParams: Parameters<Consumer["subscribe"]>;
  sinkTopic: string;
  compression?: CompressionTypes;
};

export class KafkaExactlyOnceExecutor {
  readonly manager: KafkaExactlyOnceManager;

  constructor(
    managerConfig: KafkaExactlyOnceManagerConfig,
    private readonly executorConfig: KafkaExactlyOnceExecutorConfig
  ) {
    this.manager = new KafkaExactlyOnceManager(managerConfig);
  }

  readonly init = async (): Promise<void> => {
    const consumer = await this.manager.getExactlyOnceCompatibleConsumer();

    await consumer.subscribe(...this.executorConfig.subscribeParams);

    await consumer.run({
      autoCommit: false,
      eachMessage: async (payload) => {
        const output = await this.executorConfig.processor(payload);

        const producer = await this.manager.getExactlyOnceCompatibleProducer(
          this.executorConfig.sinkTopic,
          payload.partition
        );
        const transaction = await producer.transaction();

        await transaction.send({
          topic: this.executorConfig.sinkTopic,
          messages: [output],
          acks: -1, // All brokers must ack - required for EOS.
          compression: this.executorConfig.compression,
        });

        await transaction.sendOffsets({
          consumerGroupId: this.manager.getConsumerGroupId(),
          topics: [
            {
              topic: payload.topic,
              partitions: [
                {
                  partition: payload.partition,
                  offset: payload.message.offset,
                },
              ],
            },
          ],
        });

        await transaction.commit();
      },
    });
  };
}
