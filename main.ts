import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "booking-service",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();

await producer.connect();

console.log("Producer connected to Kafka!");

await producer.send({
  topic: "booking-status-updates",
  messages: [
    {
      key: "booking-101",
      value: JSON.stringify({
        bookingId: "booking-101",
        event: "UPDATE_RECEIPT_STATUS",
      }),
    },
  ],
});

console.log("Message published successfully!");

await producer.disconnect();