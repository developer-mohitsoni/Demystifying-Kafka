// ✅ Step 1: KafkaJS Library Import Karna (Kafka se interact karne ke liye)
import { Kafka } from "kafkajs";

// ✅ Step 2: Kafka Topics Define Karna (Jin topics se data consume karna hai)
const topics = ["user.updated", "order.created"]; // Yeh 2 topics consume honge

// ✅ Step 3: UserService Class Create Karna
class UserService {
  constructor() {
    // ✅ Kafka Instance Create Karna
    this.kafka = new Kafka({
      clientId: "user-service", // Yeh service ka unique naam hai jo Kafka me register hoga
      brokers: ["localhost:9094"], // Yeh Kafka broker ka address hai (locally run ho raha hai)
    });

    // ✅ Kafka Consumer Initialize Karna
    this.consumer = this.kafka.consumer({
      groupId: "user-service-group", // Yeh consumer group ka naam hai (Kafka grouping ke liye use karta hai)
    });
  }

  // ✅ Step 4: Kafka se Connect Karna
  async connect() {
    await this.consumer.connect(); // Kafka se connection establish kar raha hai
    console.log("User Service Connected to Kafka");

    // ✅ Step 5: Topics Subscribe Karna
    await this.consumer.subscribe({
      topics, // Yeh array pass ho raha hai jo "user.updated" aur "order.created" topics ko subscribe karega
      fromBeginning: true, // Purane messages bhi consume karega (agar pehle aaye ho)
    });

    console.log(`Consumer subscribed to topics: ${topics}`);

    // ✅ Step 6: Kafka Consumer Messages Process Karna
    await this.consumer.run({
      eachMessage: async ({ message, partition, topic }) => {
        console.log(
          `Received message at ${new Date()}:`,
          message.value.toString()
        );

        try {
          // ✅ Step 7: Message ko JSON me Parse Karna
          const orderData = JSON.parse(message.value.toString());

          // ✅ Step 8: Order Processing Function Call Karna
          this.processUserUpdate(orderData);
        } catch (err) {
          console.error("Order Processing Error: ", err); // Agar JSON parse me koi error aata hai toh catch karega
        }
      },
    });
  }

  // ✅ Step 9: Kafka Consumer Disconnect Karne ka Function
  async disconnect() {
    await this.consumer.disconnect(); // Kafka consumer ko disconnect kar raha hai
    console.log("User Service Disconnected from Kafka");
  }

  // ✅ Step 10: Order Processing Function
  processUserUpdate(orderData) {
    console.log("🚀 Updating User: ", {
      orderId: orderData.orderId, // Order ID ko print kar raha hai
      userEmail: orderData.userEmail, // User ka email print kar raha hai
      items: orderData.items, // Order ke items print kar raha hai
      timestamp: new Date().toISOString(), // Current timestamp ko add kar raha hai
    });
  }
}

// ✅ Step 11: Kafka Consumer Start Karne ke liye `userService` ka Instance Create Karna
let userService;

const startUserService = async () => {
  userService = new UserService(); // `UserService` class ka ek instance bana rahe hain

  try {
    await userService.connect(); // Kafka se connect ho raha hai
    console.log("User Service Connected to Kafka");
  } catch (err) {
    console.error("User Service Initialization Error: ", err); // Agar error aata hai toh log hoga
  }
};

startUserService(); // ✅ Kafka Consumer Service Start Karna

// ✅ Step 12: Graceful Shutdown (Agar server ko manually stop karein, toh Kafka safely disconnect ho)
process.on("SIGINT", async () => {
  await userService.disconnect(); // Kafka consumer ko disconnect kar raha hai
  console.log("User Service disconnected from Kafka"); // Confirmation message print kar raha hai
  process.exit(0); // Process exit kar raha hai
});
