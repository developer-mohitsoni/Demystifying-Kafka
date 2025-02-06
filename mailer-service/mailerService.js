// KafkaJS library ko import kar rahe hain
import { Kafka } from "kafkajs";

// Kafka topic define kar rahe hain jo hum subscribe karenge
const topics = ["order.created"];

// Mailer Service class jo Kafka se connect hoke messages consume karega
class MailerService {
  constructor() {
    // Kafka client create kar rahe hain
    this.kafka = new Kafka({
      clientId: "mailer-service", // Yeh consumer ka unique ID hoga
      brokers: ["localhost:9094"], // Kafka broker ka address (localhost pe run kar raha hai)
    });

    // Kafka consumer create kar rahe hain jo ek group ka part hoga
    this.consumer = this.kafka.consumer({
      groupId: "mailer-service-group", // Yeh group ek se zyada consumer ke liye use hota hai
    });
  }

  // Kafka ke saath connection establish karne ke liye async function
  async connect() {
    await this.consumer.connect(); // Kafka consumer ko connect kar rahe hain
    console.log("Mailer Service Connected to Kafka");

    // Kafka topic subscribe kar rahe hain
    await this.consumer.subscribe({
      topics, // Humara topic "order.created" hai
      fromBeginning: true, // Purane messages bhi consume karega
    });

    // Consumer messages receive karne ke liye run karega
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(
          `Received message from topic ${topic} partition ${partition} at offset ${message.offset}`
        );

        try {
          // Message ko JSON format me parse kar rahe hain
          const orderData = JSON.parse(message.value.toString());
          console.log(orderData); // Order data ko print kar rahe hain

          // Order ka notification process karne ke liye function call kar rahe hain
          this.processOrderNotification(orderData);
        } catch (err) {
          console.error("Order Processing Error: ", err); // Error handling
        }
      },
    });
  }

  // Kafka consumer ko disconnect karne ka function
  async disconnect() {
    await this.consumer.disconnect(); // Kafka consumer ko safely disconnect kar rahe hain
    console.log("Mailer Service disconnected from Kafka");
  }

  // Order notification process karne ka function
  processOrderNotification(orderData) {
    console.log("🚀 Sending Order Notification Received: ", {
      orderId: orderData.orderId, // Order ID print kar rahe hain
      userEmail: orderData.userEmail, // User ka email print kar rahe hain
      items: orderData.items, // Order ke items print kar rahe hain
      timestamp: new Date().toISOString(), // Current timestamp add kar rahe hain
    });
  }
}

// Mailer Service ko start karne ke liye instance create kar rahe hain
let mailerService;

const startMailerService = async () => {
  mailerService = new MailerService();

  try {
    await mailerService.connect(); // Kafka se connect hone ka try kar rahe hain
    console.log("Mailer Service Connected to Kafka");
  } catch (err) {
    console.error("Mailer Service Initialization Error: ", err); // Agar error aaya toh print hoga
  }
};

// Function call kar rahe hain taaki Mailer Service start ho jaye
startMailerService();

// Graceful Shutdown setup: Jab server manually stop ho, tab Kafka safely disconnect ho
process.on("SIGINT", async () => {
  await mailerService.disconnect(); // Kafka consumer disconnect kar rahe hain
  console.log("Mailer Service disconnected from Kafka"); // Confirmation message print kar rahe hain
  process.exit(0); // Process exit kar rahe hain
});
