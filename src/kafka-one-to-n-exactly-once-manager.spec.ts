import { Partitioners } from "kafkajs";
import { KafkaOneToNExactlyOnceManager } from "./kafka-one-to-n-exactly-once-manager";
import { testKafkaConfig } from "./test-kafka-config";
import crypto from "crypto";

const randomShortString = () => crypto.randomBytes(10).toString("hex");

describe("KafkaOneToNExactlyOnceManager", () => {
  const topics = ["topic-a", "topic-b"];
  const topicA = topics[0];
  const topicB = topics[1];
  const transactionalIdPrefix = "txn-prefix";

  let service: KafkaOneToNExactlyOnceManager;

  beforeEach(() => {
    service = new KafkaOneToNExactlyOnceManager({
      transactionalIdPrefix: transactionalIdPrefix,
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

    // Clean up topics we created
    const admin = service.kafka.admin();
    const existingTopics = await admin.listTopics();
    await admin.deleteTopics({
      topics: existingTopics.filter((t) => topics.includes(t)),
    });
    await admin.disconnect();
  });

  it("round trips", async () => {
    const consumer = await service.getExactlyOnceCompatibleConsumer();
    const txn = await service.getExactlyOnceCompatibleTransaction(topicA, 1);
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

    const consumedMessageValue = await new Promise<Buffer | null>((resolve) => {
      void consumer.run({
        autoCommit: false,
        eachMessage: (payload) => {
          // If this doesn't hit, the test will time out.
          resolve(payload.message.value);

          // eachMessage returns a Promise.
          return Promise.resolve();
        },
      });
    });

    expect(consumedMessageValue?.toString()).toEqual(producedMessageValue);
  });

  describe("producer", () => {
    it("creates correct transactional id", async () => {
      // Create a unique "source topic" and "source partition" our data is generated from.
      const srcTopic = randomShortString();
      const srcPartition = Math.floor(Math.random() * 10);

      // Write a transaction.
      const txn = await service.getExactlyOnceCompatibleTransaction(
        srcTopic,
        srcPartition
      );
      await txn.commit();

      // Listen to the special, transaction state topic and make sure the
      // correct transactionalId was generated.
      const consumer = await service.getExactlyOnceCompatibleConsumer();
      await consumer.subscribe({
        topic: "__transaction_state",
        fromBeginning: true,
      });

      await new Promise(
        (resolve, reject) =>
          void consumer.run({
            autoCommit: false,
            eachMessage: (payload) => {
              try {
                // Transactional ID is after first four bytes.
                const txnId = Uint8Array.prototype.slice
                  .call(payload.message.key, 4)
                  .toString();
                const expectedTxnId =
                  transactionalIdPrefix +
                  "-" +
                  srcTopic +
                  "-" +
                  srcPartition.toString();

                if (txnId !== expectedTxnId) {
                  // Not our transaction (there will be others) -> continue.
                  return Promise.resolve();
                }

                // Test will fail if we don't resolve here (found our transaction ID).
                resolve(undefined);
              } catch (e) {
                reject(e);
              }

              return Promise.resolve();
            },
          })
      );
    });
  });

  describe("consumer", () => {
    it("returns cached consumer on second call", async () => {
      const consumer1 = await service.getExactlyOnceCompatibleConsumer();
      const consumer2 = await service.getExactlyOnceCompatibleConsumer();
      expect(consumer1).toBe(consumer2);
    });

    it("will not read uncommitted", async () => {
      // Updated to the timestamp we send a message.  We should read after this timestamp.
      let commitTimeMs = Number.MAX_SAFE_INTEGER;

      const producedMessageValue = randomShortString();
      const consumer = await service.getExactlyOnceCompatibleConsumer();

      // Start a consumer to read with a promise that resolves when we've read a message.
      await consumer.subscribe({ topic: topicB, fromBeginning: true });

      const dataConsumedPromise = new Promise(
        (resolve, reject) =>
          void consumer.run({
            autoCommit: false,
            eachMessage: (payload) => {
              try {
                // Make sure this is the produced value we're expecing.
                expect(payload.message.value?.toString()).toEqual(
                  producedMessageValue
                );

                // Make sure we're reading after the transaction was committed.
                const now = new Date().valueOf();
                expect(now).toBeGreaterThanOrEqual(commitTimeMs);

                // Resolve the dataConsumedPromise so the test can end.
                resolve(undefined);
              } catch (e) {
                // This callback won't have errors forwarded up to jest so we have
                // to pass any errors manually.
                reject(e);
              }

              // eachMessage returns a promise.
              return Promise.resolve();
            },
          })
      );

      // Send a message.
      const txn = await service.getExactlyOnceCompatibleTransaction(topicA, 1);

      await txn.send({
        topic: topicB,
        messages: [
          {
            value: producedMessageValue,
          },
        ],
      });

      // Small sleep to make failures more likely (this is a race condition failure mode).
      await new Promise((resolve) => setTimeout(resolve, 250));

      // Commit time is right before, but comfortably after send time.
      commitTimeMs = new Date().valueOf();
      await txn.commit();

      // Wait for our data consumed promise.
      await dataConsumedPromise;
    });
  });
});
