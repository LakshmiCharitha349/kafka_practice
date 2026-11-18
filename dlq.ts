import { Kafka } from "kafkajs";


const kafka = new Kafka({
  clientId: "booking-service",
  brokers: ["localhost:9092"],
});

const dlqProducer = kafka.producer();

await dlqProducer.connect();

console.log("DLQ producer connected!");

export const sendToDLQ = async (
  message: string,
  originalKey?: string,
) => {
  await dlqProducer.send({
    topic: "booking-status-updates-dlq",
    messages: [
      {
        key: originalKey,
        value: JSON.stringify({
          originalMessage: message,
          originalKey,
          failedAt: new Date().toISOString(),
          reason: "Processing failed after all retries",
        }),
      },
    ],
  });

  console.log("Message published to DLQ!");
};

const consumer = kafka.consumer({
  groupId: "failed-booking",
});

await consumer.connect();

await consumer.subscribe({
  topic: "booking-status-updates-dlq",
  fromBeginning: true,
});


await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
  const value = message.value?.toString();

  if (!value) return;

  const dlqMessage = JSON.parse(value);

  console.log({
    topic,
    partition,
    offset: message.offset,
    originalMessage: dlqMessage.originalMessage,
    failedAt: dlqMessage.failedAt,
    reason: dlqMessage.reason,
  });
},
});



