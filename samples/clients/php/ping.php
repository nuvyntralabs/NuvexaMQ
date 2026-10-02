<?php
require __DIR__ . "/client.php";
$client = connect_nuvexa("php");
$client->ping();
$client->close();
