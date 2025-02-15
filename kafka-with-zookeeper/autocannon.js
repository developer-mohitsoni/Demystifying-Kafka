import autocannon from "autocannon";
import { nanoid } from "nanoid";

const instance = autocannon({
  url: "http://localhost:3000/api/v1/orders",
  connections: 100,
  pipelining: 1,
  duration: 5,
  method: "POST",
  headers: {
    "Content-Type": "application/json",
  },
  body: JSON.stringify({
    items: [
      {
        itemId: "p1",
        quantity: 2,
      },
    ],
    price: 100,
    userEmail: "mohit@gmail.com",
    sendUserUpdate: true,
  }),

  setupClient: (client) => {
    const userEmail = `user${nanoid(10)}@gmail.com`;
    const quantity = Math.floor(Math.random() * 5) + 1;
    const price = 100 * (Math.floor(Math.random() * 5) + 1);

    const body = JSON.stringify({
      items: [
        {
          itemId: "p1",
          quantity,
        },
      ],
      price,
      userEmail,
      sendUserUpdate: true,
    });
    client.setBody(body);
  },
});

autocannon.track(instance, {
  renderProgressBar: true,
});

instance.on("done", (results) => {
  console.log("\nTest completed!");
  console.log("Mean latency: ", results.latency.mean, "ms");
  console.log("Max latency: ", results.latency.max, "ms");
  console.log("Request/sec: ", results.requests.average);
  console.log("Total requests: ", results.requests.total);
  console.log("2xx response: ", results.non2xx);
  console.log("Non-2xx response: ", results.non2xx);
});
