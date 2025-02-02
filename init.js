import { Kafka } from "kafkajs"; // ✅ KafkaJS package import kar rahe hain

// ✅ Kafka client ki identification aur broker ka setup
const clientId = "init"; // Ye ek unique naam hai jo Kafka client ko diya jata hai
const brokers = ["localhost:9094"]; // Kafka broker jo localhost pe run ho raha hai

// ✅ Kafka topics define kar rahe hain jo create karne hain
const topicsToCreate = [
  {
    name: "order.created", // Topic ka naam
    partitions: 3, // 3 partitions honge taaki load balancing ho sake
    replicationFactor: 1, // Ek hi broker pe data store hoga (no redundancy)
  },
  {
    name: "user.created",
    partitions: 1, // Ye sirf ek partition me rahega
    replicationFactor: 1, // Iska bhi backup nahi hoga
  },
];

// ✅ Kafka Client initialize kar rahe hain
const kafka = new Kafka({
  clientId, // Client ka naam
  brokers, // Kafka broker list
});

// ✅ Kafka Admin client function jo topics create karega
const startKafka = async () => {
  const admin = kafka.admin(); // Admin client initialize kiya

  try {
    await admin.connect(); // ✅ Kafka Admin Client se connection bana rahe hain
    console.log("Kafka Admin Client Connected!");

    const topics = await admin.listTopics(); // ✅ Check kar rahe hain ki existing topics kaunse hain
    console.log("Existing topics: ", topics);

    // ✅ Naye topics create kar rahe hain agar required topics nahi hain
    const created = await admin.createTopics({
      topics: topicsToCreate.map((topic) => ({
        topic: topic.name, // Topic ka naam
        numPartitions: topic.partitions || 3, // Default 3 partitions
        replicationFactor: topic.replicationFactor || 1, // Default 1 replication factor
      })),
    });

    console.log({ created }); // ✅ Check kar rahe hain ki topics successfully create hue ya nahi
  } catch (err) {
    console.error("Kafka Initialization Error: ", err); // ✅ Error handling agar Kafka setup me koi dikkat aaye
  } finally {
    await admin.disconnect(); // ✅ Kafka se safely disconnect kar rahe hain
    console.log("Kafka Admin Client Disconnected!");
  }
};

// ✅ Function call kar rahe hain taaki Kafka setup execute ho
startKafka();
