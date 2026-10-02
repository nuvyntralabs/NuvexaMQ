<?php
require __DIR__ . "/client.php";
$language = "php";
$stream = "mailbox-$language";
$durable = "box-$language";
$tail = "tail-$language";
$client = connect_nuvexa($language);
$client->ensureStream($stream, ["$stream.>"]);
$client->ensureConsumer($stream, $durable, "$stream.>");
$plain = [["content-type", "text/plain"]];
$client->publish("$stream.created", "ack me", "", $plain);
$client->publish("$stream.created", "nack me", "", $plain);
$found = [];
foreach ($client->fetch($stream, $durable) as $message) $found[$message["payload"]] = $message;
$client->ack($stream, $durable, $found["ack me"]);
echo "ack\n";
$client->nack($stream, $durable, $found["nack me"]);
echo "nack\n";
$again = null;
foreach ($client->fetch($stream, $durable) as $message) {
    if ($message["payload"] === "nack me") $again = $message;
}
if ($again === null) throw new RuntimeException("nack was not redelivered");
echo "redelivered {$again["delivery"]}\n";
$client->ack($stream, $durable, $again);
$client->reset($stream, $durable, 0);
$reset = $client->fetch($stream, $durable, 1);
if (count($reset) === 0) throw new RuntimeException("reset did not return a message");
echo "after reset offset {$reset[0]["offset"]}\n";
$client->ack($stream, $durable, $reset[0]);
$client->release($stream, $tail);
$client->ensureConsumer($stream, $tail, "", true, 1);
$client->publish("$stream.created", "tail me");
$tailed = null;
foreach ($client->fetch($stream, $tail) as $message) {
    if ($message["payload"] === "tail me") $tailed = $message;
}
if ($tailed === null) throw new RuntimeException("tail message was not delivered");
$client->ack($stream, $tail, $tailed);
$client->release($stream, $tail);
$client->ensureConsumer($stream, "from0-$language", "", false, 2, 0);
echo "offset consumer from0-$language\n";
$client->close();
