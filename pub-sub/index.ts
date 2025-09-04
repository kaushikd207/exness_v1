import redis from "redis";
import { WebSocketServer } from "ws";
const client = redis.createClient();

client.on("error", (err) => console.log("Redis Client Error", err));
await client.connect();

const wss = new WebSocketServer({ port: 8080 });
const subscriber = client.duplicate();
interface Trade {
  e: string; // Event type
  E: number; // Event time
  s: string; // Symbol
  t: number; // Trade ID
  p: string; // Price
  q: string; // Quantity
  b: number; // Buyer order ID
  a: number; // Seller order ID
  T: number; // Trade time
  m: boolean; // Is the buyer the market maker?
  M: boolean; // Ignore
}

interface PriceMap {
  [symbol: string]: Trade;
}
let PRICES: PriceMap = {};
await subscriber.connect();
console.log("Subscriber connected to Redis");
await subscriber.subscribe("trades", (message) => {
  const trade = JSON.parse(message);
  PRICES = { ...PRICES, [trade.data.s]: trade };
  console.log(trade);
});

setInterval(() => {
  wss.clients.forEach((client) => {
    if (client.readyState === client.OPEN) {
      client.send(JSON.stringify(PRICES));
    }
  });
}, 100);
