import http from "node:http";
import https from "node:https";
import { healthPort, host, managementHttpsPort, managementPort, password, user } from "./client.ts";

const language = "typescript";
const auth = "Basic " + Buffer.from(`${user}:${password}`).toString("base64");

function call(url, method = "GET", body = undefined, tls = false) {
  return new Promise((resolve, reject) => {
    const payload = body === undefined ? null : JSON.stringify(body);
    const headers = {};
    if (url.includes("/api/")) headers.Authorization = auth;
    if (payload) {
      headers["Content-Type"] = "application/json";
      headers["Content-Length"] = Buffer.byteLength(payload);
    }
    const mod = tls ? https : http;
    const req = mod.request(url, { method, headers, rejectUnauthorized: false }, (response) => {
      const chunks = [];
      response.on("data", (chunk) => chunks.push(chunk));
      response.on("end", () => {
        const raw = Buffer.concat(chunks);
        if (response.statusCode >= 400) reject(new Error(`${method} ${url} ${response.statusCode} ${raw}`));
        else { console.log(`${method} ${url} ${response.statusCode} ${raw.length} bytes`); resolve(raw); }
      });
    });
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

const health = `http://${host}:${healthPort}`;
await call(`${health}/health`);
await call(`${health}/metrics`);
const base = `http://${host}:${managementPort}`;
for (const path of ["/api/whoami", "/api/overview", "/api/connections", "/api/channels", "/api/streams", "/api/exchanges", "/api/queues", "/api/bindings", "/api/users", "/api/permissions", "/api/vhosts", "/api/policies"])
  await call(base + path);
const vhost = `vh-${language}`;
const account = `user-${language}`;
const policy = `policy-${language}`;
await call(`${base}/api/vhosts/${vhost}`, "PUT");
await call(`${base}/api/users/${account}`, "PUT", { password: "sample-pass", tags: ["management"] });
await call(`${base}/api/permissions`, "PUT", { user: account, vhost, configure: ".*", write: ".*", read: ".*" });
await call(`${base}/api/policies/${policy}`, "PUT", { vhost: "/", pattern: "sample-.*", priority: 1, messageTtlMs: 60000, maxLength: 100, deadLetterExchange: "", deadLetterRoutingKey: "" });
await call(`${base}/api/policies/${policy}?vhost=/`, "DELETE");
await call(`${base}/api/permissions?user=${account}&vhost=${vhost}`, "DELETE");
await call(`${base}/api/users/${account}`, "DELETE");
await call(`${base}/api/vhosts/${vhost}`, "DELETE");
await call(`https://${host}:${managementHttpsPort}/api/whoami`, "GET", undefined, true);
