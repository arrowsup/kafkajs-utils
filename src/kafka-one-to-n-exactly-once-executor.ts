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

// Receive a single message and return messages for a set of topics.
type Processor = (
  event: EachMessagePayload
) => Promise<{ topic: string; messages: Message[] }[]>;

type KafkaOneToNExactlyOnceExecutorConfig = {
  processor: Processor;
  subscribeParams: Parameters<Consumer["subscribe"]>;
  sendParams: Omit<
    Parameters<Transaction["send"]>,
    "messages" | "acks" | "topic"
  >;
};

/**
 * Manages exactly once transactions for a single source topic & message to a
 * set of sink topics (possible with multiple messages destined to each sink topic).
 */
export class KafkaOneToNExactlyOnceExecutor {
  readonly manager: KafkaOneToNExactlyOnceManager;

  constructor(
    managerConfig: KafkaOneToNExactlyOnceManagerConfig,
    private readonly executorConfig: KafkaOneToNExactlyOnceExecutorConfig
  ) {
    this.manager = new KafkaOneToNExactlyOnceManager(managerConfig);
  }

  readonly init = async (): Promise<void> => {
    const consumer = await this.manager.getExactlyOnceCompatibleConsumer();

    await consumer.subscribe(...this.executorConfig.subscribeParams);

    await consumer.run({
      autoCommit: false,
      eachMessage: async (payload) => {
        const outputs = await this.executorConfig.processor(payload);

        const transaction =
          await this.manager.getExactlyOnceCompatibleTransaction(
            payload.topic,
            payload.partition
          );

        for (const output of outputs) {
          await transaction.send({
            ...this.executorConfig.sendParams[0],
            messages: output.messages,
            topic: output.topic,
            acks: -1, // All brokers must ack - required for EOS.
          });
        }

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
