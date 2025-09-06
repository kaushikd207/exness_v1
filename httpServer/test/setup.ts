import { mock } from "bun:test";

type SubscriberFn = (msg: string) => void;
const subscribers: Record<string, SubscriberFn[]> = {};

mock.module("redis", () => {
  const streams: Record<
    string,
    { id: string; message: Record<string, any> }[]
  > = {};

  function createClient() {
    return {
      connect: async () => {},
      on: () => {},
      xAdd: async (
        stream: string,
        _id: string,
        payload: Record<string, any>
      ) => {
        if (!streams[stream]) streams[stream] = [];
        const id = `${Date.now()}-0`;
        streams[stream].push({ id, message: payload });
        return id;
      },
      xRead: async (keys: { key: string; id: string }[], options?: any) => {
        const result: any[] = [];
        for (const { key, id } of keys) {
          if (streams[key]?.length) {
            const messages = streams[key]
              .filter((msg) => msg.id > id)
              .map((msg) => ({ id: msg.id, message: msg.message }));
            if (messages.length) result.push({ name: key, messages });
          }
        }
        return result.length ? result : null;
      },
      duplicate: () => ({
        xAdd: async (
          stream: string,
          _id: string,
          payload: Record<string, any>
        ) => {
          if (!streams[stream]) streams[stream] = [];
          const id = `${Date.now()}-0`;
          streams[stream].push({ id, message: payload });
          return id;
        },
      }),
    };
  }

  return { createClient };
});

mock.module("nodemailer", () => {
  return {
    createTransport: () => ({
      sendMail: async () => ({ messageId: "mock-msg-id" }),
    }),
  };
});
