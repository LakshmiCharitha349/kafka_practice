
import { Kafka } from "kafkajs";
import {  sendToDLQ } from "./dlq.ts";

export const kafka = new Kafka({
  clientId: "booking-worker",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "booking-workers",
   allowAutoTopicCreation: false,
});

await consumer.connect();

await consumer.subscribe({
  topic: "booking-status-updates",
  fromBeginning: true,
});

console.log("Consumer connected and waiting for messages...");



await consumer.run({
     autoCommit: false,
  eachMessage: async ({ topic, partition, message }) => {
  const value = message.value?.toString();

  if (!value) return;

  try {
     await processWithRetry(value);

  } catch (error) {
  console.error("All retries failed. Sending to DLQ:", error);

  console.log("Before sending to DLQ");

  await sendToDLQ(value, message.key?.toString());

  console.log("After sending to DLQ");
}

    await consumer.commitOffsets([
    {
      topic,
      partition,
      offset: (BigInt(message.offset) + 1n).toString(),
    },
  ]);

  console.log(`Committed offset ${BigInt(message.offset) + 1n}`);

    console.log({
      topic,
      partition,
      offset: message.offset,
      key: message.key?.toString(),
      value: message.value?.toString(),
    });
  },
});



const processWithRetry = async (
  message: string,
  maxRetries = 3,
): Promise<void>  => {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`Attempt ${attempt}: Processing message`);

        // Simulate an API request
    if (attempt <= 3) {
        throw new Error("Simulated API failure");
    }

    console.log("Message processed successfully!");
    return;

    } catch (error) {
      console.error(`Attempt ${attempt} failed:`, error);

      if (attempt === maxRetries) {
        throw error;
      }

      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }
}

