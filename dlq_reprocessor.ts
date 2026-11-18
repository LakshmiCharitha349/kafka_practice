import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "dlq-reprocessor",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "dlq-reprocessor-group",
});

await consumer.connect();

await consumer.subscribe({
  topic: "booking-status-updates-dlq",
  fromBeginning: true,
});

console.log("DLQ reprocessor is waiting for messages...");

// resend the esages to original topic
const producer = kafka.producer();

await producer.connect();

console.log("Reprocessor producer connected!");

await consumer.run({
  autoCommit: false,

  eachMessage: async ({ topic, partition, message }) => {
    const value = message.value?.toString();

    if (!value) return;

    const dlqMessage = JSON.parse(value);

    console.log("Received failed message:", {
      originalMessage: dlqMessage.originalMessage,
      originalKey: dlqMessage.originalKey,
      reason: dlqMessage.reason,
      failedAt: dlqMessage.failedAt,
    });

    await producer.send({
  topic: "booking-status-updates",
  messages: [
    {
      key: dlqMessage.originalKey,
      value: dlqMessage.originalMessage,
    },
  ],
});

console.log("Message republished to original topic!");
await consumer.commitOffsets([
  {
    topic,
    partition,
    offset: (BigInt(message.offset) + 1n).toString(),
  },
]);

console.log("DLQ offset committed!");
  },
});

