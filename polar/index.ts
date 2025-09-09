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
      params: ["bookTicker.SOL_USDC_PERP", "bookTicker.SOL_USDC"],
    })
  );
});

ws.on("message", async function message(data) {
  const trade = JSON.parse(data.toString());
  // console.log(trade);
  const latest_price = {
    symbol: trade.data.s,
    bids: trade.data.b * 1.01,
    asks: trade.data.a * 0.99
  }

  const jsonTrade = JSON.stringify(latest_price);
  console.log(jsonTrade)
  publisherStream.xAdd("trades", "*", {
      action: "UPDATED_PRICE",
      data: jsonTrade,
    });
});



ws.on("close", function close() {
  console.log("disconnected");
});
