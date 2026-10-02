import { connect } from "./client.mjs";

const client = await connect("javascript");
await client.ping();
client.close();
