import {
  Consumer,
  EachMessagePayload,
  KafkaJSError,
  Logger,
  Message,
  Transaction,
} from "kafkajs";
import { KafkaOneToNExactlyOnceManager } from "./kafka-one-to-n-exactly-once-manager";

// Receive a single message and return messages for a set of topics.
export type KafkaJsUtilsOneToNProcessor = (
  event: EachMessagePayload
) => Promise<{ topic: string; messages: Message[] }[]>;

type KafkaOneToNExactlyOnceExecutorConfig = {
  processor: KafkaJsUtilsOneToNProcessor;
  subscribeParams: Parameters<Consumer["subscribe"]>[number];
  sendParams: Omit<
    Parameters<Transaction["send"]>[number],
    "messages" | "acks" | "topic"
  >;
};

/**
 * Manages exactly once transactions for a single source topic & message to a
 * set of sink topics (possible with multiple messages destined to each sink topic).
 *
 * Be sure to call `init` after creating this object.
 */
export class KafkaOneToNExactlyOnceExecutor {
  private readonly logger: Logger;
  private initialized = false;

  constructor(
    private readonly manager: KafkaOneToNExactlyOnceManager,
    private readonly executorConfig: KafkaOneToNExactlyOnceExecutorConfig
  ) {
    this.logger = this.manager.kafka.logger();
  }

  private readonly logPrefix = KafkaOneToNExactlyOnceExecutor.name + ": ";

  readonly init = async (): Promise<void> => {
    if (this.initialized) {
      const err = "already initialized";
      this.logger.error(this.logPrefix + err);
      throw new Error(err);
    }

    this.initialized = true;

    const consumer = await this.manager.getExactlyOnceCompatibleConsumer();

    await consumer.subscribe(this.executorConfig.subscribeParams);

    await consumer.run({
      autoCommit: false,
      eachMessage: async (payload) => {
        const outputs = await this.executorConfig.processor(payload);

        const transaction =
          await this.manager.getExactlyOnceCompatibleTransaction(
            payload.topic,
            payload.partition
          );

        try {
          for (const output of outputs) {
            await transaction.send({
              ...this.executorConfig.sendParams,
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
                    offset: (Number(payload.message.offset) + 1).toString(),
                  },
                ],
              },
            ],
          });

          await transaction.commit();
        } catch (e) {
          await transaction.abort();
          const err = e instanceof Error ? e.message : JSON.stringify(e);
          this.logger.error(this.logPrefix + "Transaction failed: " + err);
          // Swallow non-retriable errors and throw a retriable one.
          // Otherwise, in high conflict situations, we'll sometimes end up crashing and we'll stop processing.
          throw new KafkaJSError("Transaction failed.  Retrying.", {
            retriable: true,
          });
        }
      },
    });
  };
}
