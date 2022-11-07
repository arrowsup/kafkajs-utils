import { Partitioners, ResourcePatternTypes } from "kafkajs";
import { KafkaOneToNExactlyOnceManager } from "./kafka-one-to-n-exactly-once-manager";
import { testKafkaConfig } from "./test-kafka-config";

describe("KafkaOneToNExactlyOnceManager", () => {
  const topicA = "topic-a";
  const topicB = "topic-b";
  const topicC = "topic-c";

  let service: KafkaOneToNExactlyOnceManager;

  beforeEach(() => {
    service = new KafkaOneToNExactlyOnceManager({
      transactionalIdPrefix: "test",
      kafkaConfig: testKafkaConfig,
      createConsumerConfig: {
        groupId: "test-consumer-group-1",
      },
      createProducerOptions: {
        createPartitioner: Partitioners.DefaultPartitioner,
      },
    });
  });

  afterEach(async () => {
    await service.cleanUp();
  });

  it("round trips", async () => {
    const consumer = await service.getExactlyOnceCompatibleConsumer();
    const txn = await service.getExactlyOnceCompatibleTransaction(topicA, 1);
    const messageValue = "foo";

    await txn.send({
      topic: topicB,
      messages: [
        {
          value: messageValue,
        },
      ],
    });
    await txn.commit();

    await consumer.subscribe({ topic: topicB, fromBeginning: true });

    await new Promise((resolve) => {
      void consumer.run({
        autoCommit: false,
        eachMessage: async (payload) => {
          // Expect the value we sent to be received.
          expect(payload.message.value?.toString()).toEqual(messageValue);

          // If this doesn't hit, the test will time out.
          resolve(undefined);
        },
      });
    });
  }, 10000);
});
