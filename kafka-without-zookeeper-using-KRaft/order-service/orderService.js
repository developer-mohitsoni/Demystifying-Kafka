import express from "express"; // Express web framework ko import kar rahe hain
import bodyParser from "body-parser"; // Body parser ko import kar rahe hain jo incoming JSON request handle karega
import { nanoid } from "nanoid"; // nanoid ka use karenge jo unique order IDs generate karega
import { Kafka, Partitioners } from "kafkajs"; // KafkaJS package ko import kar rahe hain jo Kafka communication ke liye use hota hai

const app = express(); // Express app create kar rahe hain
const PORT = process.env.PORT || 3000; // Server ka port set kar rahe hain, agar environment variable me port nahi hai to default 3000 use karenge

app.use(bodyParser.json()); // Body-parser middleware ko use kar rahe hain jo JSON data ko parse karega

// Kafka Configuration
const kafka = new Kafka({
  clientId: "order-service", // Kafka ke client ID ko "order-service" set kar rahe hain
  brokers: ["localhost:9094", "localhost:9095", "localhost:9096"], // Kafka brokers ki list, yeh local setup pe chal raha hai
});

// Producer ko create kar rahe hain jo Kafka me messages send karega
const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner, // Legacy partitioner ka use kar rahe hain jo partitioning handle karega
});

// OrderService class jo Kafka se connect karne aur orders create karne ka kaam karega
class OrderService {
  constructor() {
    this.connectKafka(); // Constructor me Kafka se connect kar rahe hain jab service initialize hota hai
  }

  // Kafka se connect hone ka method
  async connectKafka() {
    try {
      await producer.connect(); // Kafka producer ko connect kar rahe hain
    } catch (err) {
      console.error("Kafka Connection Error: ", err); // Agar connection me koi error aaye to usko handle kar rahe hain
    }
  }

  // Order create karne ka method
  async createOrder(orderData) {
    try {
      const key = orderData.userEmail || Date.now().toString(); // Agar orderData me user email hai to usko key bana rahe hain, agar nahi to timestamp ko key bana rahe hain
      console.log("Sending order.created event"); // Kafka ko message bhej rahe hain "order.created" topic pe

      const now = new Date().toISOString(); // Timestamp generate kar rahe hain

      await producer.send({
        topic: "order.created", // Topic ka naam jahan message bhejna hai
        messages: [
          {
            key, // Key define kar rahe hain jo user ke email ya timestamp hoga
            value: JSON.stringify({
              // Order data ko stringify kar rahe hain jo Kafka me send karenge
              orderId: nanoid(10), // Unique order ID generate kar rahe hain
              ...orderData, // Order data ko spread kar rahe hain
              timeStamp: now, // Order ka timestamp bhi add kar rahe hain
            }),
          },
        ],
      });

      return { success: true, orderId: key }; // Agar sab kuch sahi se ho gaya to success return kar rahe hain order ID ke saath
    } catch (err) {
      console.error("Order Creation Error: ", err); // Agar order create karte waqt koi error aaye to handle kar rahe hain
      throw err; // Error ko throw kar rahe hain
    }
  }

  // User ko update karne ka method
  async updateUser(userEmail, orderId) {
    try {
      const key = userEmail; // User ka email ko key bana rahe hain
      console.log("Sending user.updated event"); // Kafka ko message bhej rahe hain "user.updated" topic pe

      await producer.send({
        topic: "user.updated", // Topic ka naam
        messages: [
          {
            key, // Key jo user ka email hoga
            value: JSON.stringify({
              orderId,
              userEmail,
              updatedAt: new Date().toISOString(), // Update timestamp generate kar rahe hain
            }),
          },
        ],
      });

      return { success: true, orderId: key }; // Agar user update successfully ho jata hai to success return kar rahe hain
    } catch (err) {
      console.error("User Update Error: ", err); // Agar user update me koi error aaye to usko handle kar rahe hain
      throw err; // Error ko throw kar rahe hain
    }
  }
}

// OrderService class ka instance bana rahe hain
const orderService = new OrderService();

// API Routes
app.post("/api/v1/orders", async (req, res) => {
  try {
    const order = await orderService.createOrder(req.body); // Incoming request body se order create kar rahe hain

    if (req.body.sendUserUpdate) {
      // Agar `sendUserUpdate` true hai to user ko update kar rahe hain
      await orderService.updateUser(req.body.userEmail, order.orderId);
    }

    res.status(201).json(order); // Response me order ko return kar rahe hain
  } catch (err) {
    res.status(500).json({ error: "Order creation failed" }); // Agar koi error aaye to failure message bhej rahe hain
  }
});

// Server ko start kar rahe hain
app.listen(PORT, () => {
  console.log(`Order Server is running on port ${PORT}`); // Server ka status log kar rahe hain
});

// Graceful Shutdown: Jab server ko manually stop kiya jaye, Kafka se safely disconnect hoga
process.on("SIGINT", async () => {
  await producer.disconnect(); // Kafka producer ko disconnect kar rahe hain
  console.log("Order Service disconnected from Kafka"); // Kafka se disconnect hone ka message print kar rahe hain
  process.exit(0); // Process exit kar rahe hain
});
