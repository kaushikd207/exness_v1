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

// let BALANCE : Map<string, number>= new Map()

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
// await loadSnapshot();

// ---------------- Publisher -----------------
async function publishResponse(orderId: string, payload: any) {
  await new Promise((r) => setTimeout(r, 1000));
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
  const streams = await client.xRead(
    {
      key: "trades",
      id: "$",
    },
    {
      BLOCK: 0,
      COUNT: 1,
    }
  );

  if (!streams) continue;

  for (const stream of streams[0].messages) {
    const { action, data } = stream.message;

    switch (action) {
      case "UPDATED_PRICE": {
        const updatePrice = JSON.parse(data);
        UPDATE_PRICE.set(updatePrice.symbol, updatePrice);

        // await checkLiquidations(updatePrice);
        break;
      }

      case "CREATE_ORDER": {
        const parsedData = JSON.parse(data);

        const margin = parseFloat(parsedData.margin);
        const buyingCapacity = margin * parseFloat(parsedData.leverage);
        let entryPrice = UPDATE_PRICE.has(parsedData.asset)
          ? parsedData.type == "long"
            ? UPDATE_PRICE.get(parsedData.asset).bids
            : UPDATE_PRICE.get(parsedData.asset).asks
          : NaN;

        // normalize type
        let orderType = parsedData.type?.toUpperCase();
        if (orderType === "BUY") orderType = "LONG";
        if (orderType === "SELL") orderType = "SHORT";
        if (isNaN(entryPrice)) {
          await publishResponse(parsedData.orderId, {
            status: "error",
            reason: "No market price available",
          });
          break;
        }

        if (margin > BALANCE) {
          await publishResponse(parsedData.orderId, {
            status: "error",
            reason: "Insufficient funds",
          });
        } else {
          BALANCE -= margin;
          const newOrder = {
            ...parsedData,
            type: orderType,
            qty: buyingCapacity / entryPrice,
            entryPrice,
          };
          OPEN_ORDERS.push(newOrder);

          await publishResponse(parsedData.orderId, {
            status: "success",
            order: newOrder,
            balance: BALANCE,
          });
        }
        break;
      }

      case "CLOSE_ORDER": {
        const parsedData = JSON.parse(data);
        const order = OPEN_ORDERS.find((o) => o.orderId === parsedData.orderId);
        console.log("order--", order);
        if (order) {
          const orderDetails = UPDATE_PRICE.get(order.asset);
          console.log("order details", orderDetails);
          const profit =
            order.type === "LONG"
              ? orderDetails.asks - order.entryPrice
              : orderDetails.bids - order.entryPrice;

          console.log("profit", profit);
          BALANCE = BALANCE + parseFloat(order.margin) + profit;
          OPEN_ORDERS = OPEN_ORDERS.filter(
            (o) => o.orderId !== parsedData.orderId
          );

          await publishResponse(parsedData.orderId, {
            status: "closed",
            orderId: parsedData.orderId,
            balance: BALANCE,
            profit: profit,
          });
        } else {
          await publishResponse(parsedData.orderId, {
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

    // await client.xAck(STREAM, GROUP, msg.id);
  }
}
