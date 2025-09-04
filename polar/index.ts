import WebSocket from "ws";
import redis from "redis";
const ws = new WebSocket("wss://ws.backpack.exchange");

ws.on("error", console.error);

const client = redis.createClient();
client.on("error", (err) => console.log("Redis Client Error", err));
await client.connect();
const publisherStream = client.duplicate();
await publisherStream.connect();

const publisher = client.duplicate();
await publisher.connect();

ws.on("open", function open() {
  console.log("ws connected");
  ws.send(
    JSON.stringify({
      method: "SUBSCRIBE",
      params: ["trade.SOL_USDC_PERP", "trade.BTC_USDT_PERP"],
    })
  );
});

ws.on("message", async function message(data) {
  const trade = JSON.parse(data.toString());
  console.log(trade);
  const jsonTrade = JSON.stringify(trade);
  publisher.publish("trades", jsonTrade);

  addUpdatedPrice(jsonTrade);

  //   if (trade.e === "trade") {
  //     console.log(`Trade: Price - ${trade.p}, Quantity - ${trade.q}`);
  //   }
});

const addUpdatedPrice = (jsonTrade: any) => [
  setTimeout(async () => {
    await publisherStream.xAdd("trades", "*", {
      action: "UPDATED_PRICE",
      updatedPrice: jsonTrade,
    });
  }, 100),
];

ws.on("close", function close() {
  console.log("disconnected");
});
