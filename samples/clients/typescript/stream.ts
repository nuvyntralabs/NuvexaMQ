import { connect } from "./client.ts";

const language = "typescript";
const stream = `catalog-${language}`;
const client = await connect(language);
await client.ensureStream(stream, [`${stream}.>`], 2, 86_400_000, 1_048_576, 65_536);
const body = Buffer.from('{"id":1}');
const headers = [["content-type", Buffer.from("application/json")]];
const keyed = await client.publish(`${stream}.created`, body, "alpha", headers);
const first = await client.publish(`${stream}.created`, body);
const second = await client.publish(`${stream}.created`, body);
if (!keyed.length || first[0].partition === second[0].partition) throw new Error("round-robin did not use both partitions");
client.close();
