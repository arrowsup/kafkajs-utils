import { KafkaConfig } from "kafkajs";

export const testKafkaConfig: KafkaConfig = {
  brokers: ["127.0.0.1:9092"],
  clientId: "test-client",
};
