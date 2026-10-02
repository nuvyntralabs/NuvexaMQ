<?php
require __DIR__ . "/client.php";
$language = "php";
$host = nuvexa_env("NUVEXA_HOST", "127.0.0.1");
$health = intval(nuvexa_env("NUVEXA_HEALTH_PORT", "5762"));
$management = intval(nuvexa_env("NUVEXA_MANAGEMENT_PORT", "5763"));
$https = intval(nuvexa_env("NUVEXA_MANAGEMENT_HTTPS_PORT", "5764"));
nuvexa_http("GET", "http://$host:$health/health", null, false);
nuvexa_http("GET", "http://$host:$health/metrics", null, false);
$base = "http://$host:$management";
foreach (["/api/whoami", "/api/overview", "/api/connections", "/api/channels", "/api/streams", "/api/exchanges", "/api/queues", "/api/bindings", "/api/users", "/api/permissions", "/api/vhosts", "/api/policies"] as $path) {
    nuvexa_http("GET", $base . $path);
}
$vhost = "vh-$language";
$account = "user-$language";
$policy = "policy-$language";
nuvexa_http("PUT", "$base/api/vhosts/$vhost");
nuvexa_http("PUT", "$base/api/users/$account", json_encode(["password" => "sample-pass", "tags" => ["management"]]));
nuvexa_http("PUT", "$base/api/permissions", json_encode(["user" => $account, "vhost" => $vhost, "configure" => ".*", "write" => ".*", "read" => ".*"]));
nuvexa_http("PUT", "$base/api/policies/$policy", json_encode(["vhost" => "/", "pattern" => "sample-.*", "priority" => 1, "messageTtlMs" => 60000, "maxLength" => 100, "deadLetterExchange" => "", "deadLetterRoutingKey" => ""]));
nuvexa_http("DELETE", "$base/api/policies/$policy?vhost=/");
nuvexa_http("DELETE", "$base/api/permissions?user=$account&vhost=$vhost");
nuvexa_http("DELETE", "$base/api/users/$account");
nuvexa_http("DELETE", "$base/api/vhosts/$vhost");
nuvexa_http("GET", "https://$host:$https/api/whoami");
