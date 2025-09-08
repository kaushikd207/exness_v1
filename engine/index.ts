import redis from "redis";

const client = redis.createClient();
await client.connect();
console.log("Engine connected to Redis ✅");

const publisher = client.duplicate();
await publisher.connect();

const STREAM = "trades";
const GROUP = "engine-group";
const CONSUMER = "engine-1";
const SNAPSHOT_KEY = "engine_snapshot";

try {
  await client.xGroupCreate(STREAM, GROUP, "0", { MKSTREAM: true });
  console.log("Consumer group created");
} catch (err: any) {
  if (err.message.includes("BUSYGROUP")) {
    await client.xGroupDestroy(STREAM, GROUP);
    await client.xGroupCreate(STREAM, GROUP, "0");
  } else {
    throw err;
  }
}

let BALANCE = 5000;
let OPEN_ORDERS: any[] = [];
let UPDATE_PRICE: Map<string, any> = new Map();

// ---------------- Snapshot -----------------
async function loadSnapshot() {
  const snapshot = await client.get(SNAPSHOT_KEY);
  if (snapshot) {
    const parsed = JSON.parse(snapshot);
    BALANCE = parsed.balance ?? BALANCE;
    OPEN_ORDERS = parsed.openOrders ?? [];

    if (parsed.updatePrice) {
      UPDATE_PRICE = new Map();
      if (Array.isArray(parsed.updatePrice)) {
        parsed.updatePrice.forEach((item: any) => {
          if (item.s && item.p) {
            UPDATE_PRICE.set(item.s, item.p);
          }
        });
      } else if (parsed.updatePrice instanceof Object) {
        Object.entries(parsed.updatePrice).forEach(([key, value]) => {
          UPDATE_PRICE.set(key, value);
        });
      }
    }
  }
}

async function saveSnapshot() {

  const updatePriceObject = Object.fromEntries(UPDATE_PRICE);

  const snapshot = {
    balance: BALANCE,
    openOrders: OPEN_ORDERS,
    updatePrice: updatePriceObject,
    timestamp: Date.now(),
  };
  await client.set(SNAPSHOT_KEY, JSON.stringify(snapshot));
}
setInterval(saveSnapshot, 5000);
await loadSnapshot();

// ---------------- Publisher -----------------
async function publishResponse(orderId: string, payload: any) {
  console.log("Published res ", JSON.stringify(payload));
  await publisher.xAdd("trade_responses", "*", {
    orderId,
    response: JSON.stringify(payload),
  });
}

// ---------------- Liquidation Logic -----------------
async function checkLiquidations(latestPrice: any) {
  const { s: symbol, p: price } = latestPrice.data;
  const currentPrice = parseFloat(price);

  for (const order of [...OPEN_ORDERS]) {
    if (order.asset !== symbol) continue;

    const entryPrice = parseFloat(order.entryPrice);
    const margin = parseFloat(order.margin);
    const leverage = parseFloat(order.leverage);

    const notional = margin * leverage;
    let pnl = 0;

    if (order.type.toUpperCase() === "LONG") {
      pnl = (currentPrice - entryPrice) * (notional / entryPrice);
    } else if (order.type.toUpperCase() === "SHORT") {
      pnl = (entryPrice - currentPrice) * (notional / entryPrice);
    }

    if (pnl <= -margin) {
      OPEN_ORDERS = OPEN_ORDERS.filter((o) => o.orderId !== order.orderId);

      await publishResponse(order.orderId, {
        status: "liquidated",
        orderId: order.orderId,
        symbol,
        entryPrice,
        liquidationPrice: currentPrice,
        balance: BALANCE,
      });
    }
  }
}

// ---------------- Engine Main Loop -----------------
while (true) {
  const res: any = await client.xReadGroup(
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
          UPDATE_PRICE.set(updatePrice.data.s, updatePrice.data.p);

          // Optional: Limit map size if needed (keep only recent symbols)
          if (UPDATE_PRICE.size > 1000) {
            const firstKey:any = UPDATE_PRICE.keys().next().value;
            UPDATE_PRICE.delete(firstKey);
          }

          await checkLiquidations(updatePrice);
          break;
        }

        case "CREATE_ORDER": {
          const margin = parseFloat(data.margin);
          let entryPrice = UPDATE_PRICE.has(data.asset)
            ? parseFloat(UPDATE_PRICE.get(data.asset))
            : NaN;

          // normalize type
          let orderType = data.type?.toUpperCase();
          if (orderType === "BUY") orderType = "LONG";
          if (orderType === "SELL") orderType = "SHORT";
          if (isNaN(entryPrice)) {
            await publishResponse(data.orderId, {
              status: "error",
              reason: "No market price available",
            });
            break;
          }

          if (margin > BALANCE) {
            await publishResponse(data.orderId, {
              status: "error",
              reason: "Insufficient funds",
            });
          } else {
            BALANCE -= margin;
            const newOrder = {
              ...data,
              type: orderType,
              margin,
              entryPrice,
            };
            OPEN_ORDERS.push(newOrder);

            await publishResponse(data.orderId, {
              status: "success",
              order: newOrder,
              balance: BALANCE,
            });
          }
          break;
        }

        case "CLOSE_ORDER": {
          const order = OPEN_ORDERS.find((o) => o.orderId === data.orderId);
          if (order) {
            BALANCE += order.margin;
            OPEN_ORDERS = OPEN_ORDERS.filter((o) => o.orderId !== data.orderId);

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
          await publishResponse(data.orderId, {
            balance: BALANCE,
          });
          break;
        }

        case "CHECK_USD_BALANCE": {
          await publishResponse(data.orderId, {
            usdBalance: BALANCE,
          });
          break;
        }

        default:
      }

      await client.xAck(STREAM, GROUP, msg.id);
    }
  }
}
