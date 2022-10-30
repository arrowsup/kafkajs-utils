import {
  CompressionTypes,
  Consumer,
  EachMessagePayload,
  Message,
  ProducerRecord,
  Transaction,
} from "kafkajs";
import {
  KafkaOneToNExactlyOnceManager,
  KafkaOneToNExactlyOnceManagerConfig,
} from "./kafka-one-to-n-exactly-once-manager";

type KafkaExactlyOnceOneToOneExecutorConfig = {
  processor: (event: EachMessagePayload) => Promise<Message>;
  subscribeParams: Parameters<Consumer["subscribe"]>;
  sendParams: Omit<Parameters<Transaction["send"]>, "messages" | "acks">;
};

export class KafkaExactlyOnceOneToOneExecutor {
  readonly manager: KafkaOneToNExactlyOnceManager;

  constructor(
    managerConfig: KafkaOneToNExactlyOnceManagerConfig,
    private readonly executorConfig: KafkaExactlyOnceOneToOneExecutorConfig
  ) {
    this.manager = new KafkaOneToNExactlyOnceManager(managerConfig);
  }

  readonly init = async (): Promise<void> => {
    const consumer = await this.manager.getExactlyOnceCompatibleConsumer();

    await consumer.subscribe(...this.executorConfig.subscribeParams);

    await consumer.run({
      autoCommit: false,
      eachMessage: async (payload) => {
        const output = await this.executorConfig.processor(payload);

        const transaction =
          await this.manager.getExactlyOnceCompatibleTransaction(
            payload.topic,
            payload.partition
          );

        await transaction.send({
          ...this.executorConfig.sendParams[0],
          messages: [output],
          acks: -1, // All brokers must ack - required for EOS.
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
