import express from "express";
import nodemailer from "nodemailer";
import jwt from "jsonwebtoken";
import redis from "redis";
import { v4 as uuidv4 } from "uuid";

const app = express();
app.use(express.json());

const client = redis.createClient();
await client.connect();

const publisher = redis.createClient();
await publisher.connect();

client.on("error", (err) => console.log("Redis Client Error", err));

async function sendAndWaitResponse(
  orderId: string,
  payload: Record<string, any>,
  res: any
) {
  return new Promise(async (resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error("Timeout: No response from engine"));
    }, 10000);
    await publisher.xAdd("trades", "*", payload);

    try {
      let lastId = "$";

      while (true) {
        const messages = await client.xRead(
          [{ key: "trade_responses", id: lastId }],
          { BLOCK: 5000, COUNT: 1 }
        );

        if (!messages) continue;

        for (const stream of messages) {
          for (const msg of stream.messages) {
            lastId = msg.id;
            const { orderId: respOrderId, response } = msg.message;
            if (respOrderId === orderId) {
              clearTimeout(timeout);
              res.json({ message: JSON.parse(response) });
              return resolve(true);
            }
          }
        }
      }
    } catch (err) {
      clearTimeout(timeout);
      reject(err);
    }
  });
}

app.post("/signup", (req, res) => {
  const { email } = req.body;

  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!email || !emailRegex.test(email)) {
    return res.status(400).json({ message: "Invalid email format" });
  }

  const token = jwt.sign({ email }, "kaushik", { expiresIn: "1h" });
  console.log(token);

  const transporter = nodemailer.createTransport({
    host: "smtp.gmail.com",
    port: 587,
    secure: false,
    auth: {
      user: process.env.USER_NAME,
      pass: process.env.PASSWORD,
    },
  });

  transporter.sendMail({
    from: "shemdas21@gmail.com",
    to: email,
    subject: "Please Verify your email",
    text: `http://localhost:3000/verify/${token}`,
    html: `Please verify this mail <a href="http://localhost:3000/verify/${token}">Verify</a>`,
  });

  res.json({ message: "Verification email sent" });
});

app.get("/verify/:token", (req, res) => {
  const { token } = req.params;
  jwt.verify(token, "kaushik", (err) => {
    if (err) {
      return res.status(400).json({ message: "Invalid or expired token" });
    }
    res.json({ message: "Email verified successfully" });
  });
});

app.post("/api/v1/trade/create", async (req, res) => {
  const { asset, type, margin, leverage, slippage } = req.body;
  if (!asset || !type || !margin || !leverage || !slippage) {
    return res.status(400).json({ message: "All fields are required" });
  }

  const orderId = uuidv4();
  await sendAndWaitResponse(
    orderId,
    {
      action: "CREATE_ORDER",
      orderId,
      asset,
      type,
      margin,
      slippage,
      leverage,
    },
    res
  );
});

app.post("/api/v1/trade/close", async (req, res) => {
  const { orderId, userId } = req.body;
  if (!orderId) {
    return res.status(400).json({ message: "orderId is required" });
  }

  await sendAndWaitResponse(
    orderId,
    { action: "CLOSE_ORDER", orderId, userId },
    res
  );
});

app.get("/api/v1/balance", async (req, res) => {
  const { userId } = req.query;
  const orderId = uuidv4();

  await sendAndWaitResponse(
    orderId,
    { action: "CHECK_BALANCE", orderId, userId },
    res
  );
});

app.get("/api/v1/balance_usd", async (req, res) => {
  const { userId } = req.query;
  const orderId = uuidv4();

  await sendAndWaitResponse(
    orderId,
    { action: "CHECK_USD_BALANCE", orderId, userId },
    res
  );
});

app.listen(3000, () => {
  console.log("Server started on port 3000");
});

export default app;
