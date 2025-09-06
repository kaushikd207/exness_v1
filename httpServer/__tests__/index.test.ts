import "../test/setup";
import app from "../index";
import request from "supertest";
import { describe, it, expect } from "bun:test";
import jwt from "jsonwebtoken";

describe("HTTP API + mocked engine", () => {
  it("POST /signup endpoint should send mail", async () => {
    const res = await request(app)
      .post("/signup")
      .send({ email: "kaushikd207@gmail.com" });

    expect(res.status).toBe(200);
    expect(res.body.message).toBe("Verification email sent");
  });

  it("POST /signup should send error if invalid email", async () => {
    const res = await request(app)
      .post("/signup")
      .send({ email: "not-an-email" });

    expect(res.status).toBe(400);
    expect(res.body.message).toBe("Invalid email format");
  });

  it("GET /verify/:should verify token if token is correct", async () => {
    const token = jwt.sign({ email: "x@y.com" }, "kaushik", {
      expiresIn: "1h",
    });
    const res = await request(app).get(`/verify/${token}`);
    expect(res.status).toBe(200);
    expect(res.body.message).toBe("Email verified successfully");
  });

  it("POST /api/v1/trade/create -> should send success if created an order", async () => {
    const res = await request(app).post("/api/v1/trade/create").send({
      asset: "BTC_USDT",
      type: "LONG",
      margin: "100",
      leverage: "10",
      slippage: "0.1",
    });

    expect(res.status).toBe(200);
    expect(res.body.message.status).toBe("success");
  });

  it("POST /api/v1/trade/close -> should send success if closed an order", async () => {
    const res = await request(app)
      .post("/api/v1/trade/close")
      .send({ orderId: "2562", userId: "256655", action: "CLOSE_ORDER" });

    expect(res.status).toBe(200);
  });

  it("GET /api/v1/balance -> should return balance", async () => {
    const res = await request(app)
      .get("/api/v1/balance")
      .query({ userId: "user1", orderId: "123", action: "CHECK_BALANCE" });

    await expect(res.status).toBe(200);
  });

  it("GET /api/v1/balance_usd -> should return usd balance", async () => {
    const res = await request(app)
      .get("/api/v1/balance_usd")
      .query({ userId: "user1", orderId: "123", action: "CHECK_USD_BALANCE" });

    await expect(res.status).toBe(200);
  });
});
