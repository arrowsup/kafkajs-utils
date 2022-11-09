import { Partitioners } from "kafkajs";
import { KafkaOneToNExactlyOnceManager } from "./kafka-one-to-n-exactly-once-manager";
import { testKafkaConfig } from "./test-kafka-config";
import crypto from "crypto";
import { testConsumptionHelper } from "./test-consumption-helper";

const randomShortString = () => crypto.randomBytes(10).toString("hex");

describe("KafkaOneToNExactlyOnceManager", () => {
  const topics = ["topic-a", "topic-b"];
  const topicA = topics[0];
  const topicB = topics[1];
  const transactionalIdPrefix = "txn-prefix-mngr";

  let manager: KafkaOneToNExactlyOnceManager;

  beforeEach(() => {
    manager = new KafkaOneToNExactlyOnceManager({
      transactionalIdPrefix: transactionalIdPrefix,
      kafkaConfig: testKafkaConfig,
      createConsumerConfig: {
        groupId: "test-consumer-group-mngr",
      },
      createProducerOptions: {
        createPartitioner: Partitioners.DefaultPartitioner,
      },
    });
  });

  afterEach(async () => {
    await manager.cleanUp();

    // Clean up topics we created
    const admin = manager.kafka.admin();
    const existingTopics = await admin.listTopics();
    await admin.deleteTopics({
      topics: existingTopics.filter((t) => topics.includes(t)),
    });
    await admin.disconnect();
  });

  it("round trips", async () => {
    const consumer = await manager.getExactlyOnceCompatibleConsumer();
    const txn = await manager.getExactlyOnceCompatibleTransaction(topicA, 1);
    const producedMessageValue = randomShortString();

    // Send a single string.
    await txn.send({
      topic: topicB,
      messages: [
        {
          value: producedMessageValue,
        },
      ],
    });
    await txn.commit();

    // Read the string, then make sure it matches.
    await consumer.subscribe({ topic: topicB, fromBeginning: true });
    const consumedMessageValue = await testConsumptionHelper<Buffer | null>(
      consumer,
      (resolve) => (payload) => Promise.resolve(resolve(payload.message.value))
    );
    expect(consumedMessageValue?.toString()).toEqual(producedMessageValue);
  });

  describe("producer", () => {
    let producerSpy: jest.SpyInstance;

    beforeEach(() => {
      producerSpy = jest.spyOn(manager.kafka, "producer");
    });

    it("calls producer w/ correct params", async () => {
      // Create a unique "source topic" and "source partition" our data is generated from.
      const srcTopic = randomShortString();
      const srcPartition = Math.floor(Math.random() * 10);

      // Create a producer.
      await manager.getExactlyOnceCompatibleTransaction(srcTopic, srcPartition);

      // Verified we passed the right params.
      const expectedTxnId =
        transactionalIdPrefix + "-" + srcTopic + "-" + srcPartition.toString();

      expect(producerSpy).toHaveBeenCalledWith({
        createPartitioner: Partitioners.DefaultPartitioner,
        idempotent: true,
        maxInFlightRequests: 1,
        transactionalId: expectedTxnId,
      });
    });
  });

  describe("consumer", () => {
    let consumerSpy: jest.SpyInstance;

    beforeEach(() => {
      consumerSpy = jest.spyOn(manager.kafka, "consumer");
    });

    it("returns cached consumer on second call", async () => {
      const consumer1 = await manager.getExactlyOnceCompatibleConsumer();
      const consumer2 = await manager.getExactlyOnceCompatibleConsumer();
      expect(consumer1).toBe(consumer2);
    });

    it("calls consumer with the right params", async () => {
      await manager.getExactlyOnceCompatibleConsumer();

      expect(consumerSpy).toHaveBeenCalledWith({
        groupId: "test-consumer-group-mngr",
        readUncommitted: false,
      });
    });
  });
});
