<?php
require __DIR__ . "/client.php";
$language = "php";
$stream = "catalog-$language";
$client = connect_nuvexa($language);
$client->ensureStream($stream, ["$stream.>"], 2, 86400000, 1048576, 65536);
$body = '{"id":1}';
$headers = [["content-type", "application/json"]];
$keyed = $client->publish("$stream.created", $body, "alpha", $headers);
$first = $client->publish("$stream.created", $body);
$second = $client->publish("$stream.created", $body);
if (count($keyed) === 0 || $first[0]["partition"] === $second[0]["partition"]) {
    throw new RuntimeException("round-robin did not use both partitions");
}
$client->close();
