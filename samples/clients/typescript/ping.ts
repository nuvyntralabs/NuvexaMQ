import { connect } from "./client.ts";

const client = await connect("typescript");
await client.ping();
client.close();
