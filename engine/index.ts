// engine.ts
import redis from "redis";

const client = redis.createClient();
await client.connect();
console.log("Engine connected to Redis ✅");

const publisher = client.duplicate();
await publisher.connect();

const STREAM = "trades";
const GROUP = "engine-group";
const CONSUMER = "engine-1";

try {
  await client.xGroupCreate(STREAM, GROUP, "0", { MKSTREAM: true });
  console.log("Consumer group created");
} catch (err: any) {
  if (err.message.includes("BUSYGROUP")) {
    console.log("Consumer group already exists");
  } else {
    throw err;
  }
}

let BALANCE = 5000;
let OPEN_ORDERS: any[] = [];
let UPDATE_PRICE: any[] = [];

async function publishResponse(orderId: string, payload: any) {
  await publisher.publish(`trade_response_${orderId}`, JSON.stringify(payload));
}

while (true) {
  const res = await client.xReadGroup(
    GROUP,
    CONSUMER,
    [{ key: STREAM, id: ">" }],
    { COUNT: 10, BLOCK: 5000 }
  );

  if (!res) continue;

  for (const stream of res) {
    for (const msg of stream.messages) {
      const { action, ...data } = msg.message;

      switch (action) {
        case "UPDATED_PRICE": {
          const updatePrice = JSON.parse(data.updatedPrice);
          UPDATE_PRICE.push(updatePrice);
          console.log("📈 Price updated", updatePrice);
          break;
        }

        case "CREATE_ORDER": {
          const margin = parseFloat(data.margin);
          if (margin > BALANCE) {
            console.log("Insufficient funds for order", data.orderId);
            await publishResponse(data.orderId, {
              status: "error",
              reason: "Insufficient funds",
            });
          } else {
            BALANCE -= margin; // lock margin
            OPEN_ORDERS.push({ ...data, margin });
            console.log(
              ` Order created: ${data.orderId}, Locked margin: ${margin}, Remaining balance: ${BALANCE}`
            );
            await publishResponse(data.orderId, {
              status: "success",
              order: data,
              balance: BALANCE,
            });
          }
          break;
        }

        case "CLOSE_ORDER": {
          const order = OPEN_ORDERS.find((o) => o.orderId === data.orderId);
          if (order) {
            BALANCE += order.margin; // unlock margin
            OPEN_ORDERS = OPEN_ORDERS.filter((o) => o.orderId !== data.orderId);
            console.log(
              `Order closed: ${data.orderId}, Unlocked margin: ${order.margin}, Balance: ${BALANCE}`
            );
            await publishResponse(data.orderId, {
              status: "closed",
              orderId: data.orderId,
              balance: BALANCE,
            });
          } else {
            await publishResponse(data.orderId, {
              status: "error",
              reason: "Order not found",
            });
          }
          break;
        }

        case "CHECK_BALANCE": {
          console.log(" Balance check", data.userId, BALANCE);
          await publishResponse(data.orderId, {
            balance: BALANCE,
          });
          break;
        }

        case "CHECK_USD_BALANCE": {
          console.log("USD Balance check", data.userId, BALANCE);

          await publishResponse(data.orderId, {
            usdBalance: BALANCE,
          });
          break;
        }

        default:
          console.log("⚠️ Unknown action", msg.message);
      }

      await client.xAck(STREAM, GROUP, msg.id);
    }
  }
}
