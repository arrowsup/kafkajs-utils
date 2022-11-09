import { Partitioners } from "kafkajs";
import {
  KafkaJsUtilsOneToNProcessor,
  KafkaOneToNExactlyOnceExecutor,
} from "./kafka-one-to-n-exactly-once-executor";
import { testConsumptionHelper } from "./test-consumption-helper";
import { testKafkaConfig } from "./test-kafka-config";

describe("KafkaOneToNExactlyOnceExecutor", () => {
  const topics = ["topic-1", "topic-2", "topic-3"];
  const topic1 = topics[0];
  const topic2 = topics[1];
  const topic3 = topics[2];
  const transactionalIdPrefix = "txn-prefix-exec";

  let executor: KafkaOneToNExactlyOnceExecutor;
  let processor: KafkaJsUtilsOneToNProcessor;

  beforeEach(() => {
    executor = new KafkaOneToNExactlyOnceExecutor(
      {
        transactionalIdPrefix: transactionalIdPrefix,
        kafkaConfig: testKafkaConfig,
        createConsumerConfig: {
          groupId: "test-consumer-group-exec",
        },
        createProducerOptions: {
          createPartitioner: Partitioners.DefaultPartitioner,
        },
      },
      {
        processor: (event) => {
          // Dispatch to test's processor.
          return processor(event);
        },
        subscribeParams: {
          topic: topic1,
          fromBeginning: true,
        },
        sendParams: {},
      }
    );
  });

  afterEach(async () => {
    await executor.cleanUp();

    // Clean up topics we created
    const admin = executor.manager.kafka.admin();
    const existingTopics = await admin.listTopics();
    await admin.deleteTopics({
      topics: existingTopics.filter((t) => topics.includes(t)),
    });
    await admin.disconnect();
  });

  describe("one to many", () => {
    const t2Value = "2";
    const t3Value = "3";

    beforeEach(async () => {
      processor = (event) => {
        return Promise.resolve([
          {
            topic: topic2,
            messages: [
              {
                key: event.message.key,
                value: t2Value,
              },
            ],
          },
          {
            topic: topic3,
            messages: [
              {
                key: event.message.key,
                value: t3Value,
              },
            ],
          },
        ]);
      };

      await executor.init();
    });

    it("executes", async () => {
      const key = "a key";
      const sendTxn =
        await executor.manager.getExactlyOnceCompatibleTransaction(topic1, 0);
      await sendTxn.send({
        topic: topic1,
        messages: [
          {
            key,
            value: "a value",
          },
        ],
      });
      await sendTxn.commit();

      const consumer = executor.manager.kafka.consumer({
        groupId: "exec-one-to-n-test-group",
        readUncommitted: false,
      });

      try {
        await consumer.subscribe({
          topics: [topic2, topic3],
          fromBeginning: true,
        });

        const seenValues: string[] = [];

        await testConsumptionHelper(consumer, (resolve) => (payload) => {
          expect(payload.message.key?.toString()).toEqual(key);

          if (payload.message.value) {
            seenValues.push(payload.message.value?.toString());
          }

          if (seenValues.includes(t2Value) && seenValues.includes(t3Value)) {
            resolve(undefined);
          }

          return Promise.resolve();
        });
      } finally {
        await consumer.disconnect();
      }
    });
  });
});
