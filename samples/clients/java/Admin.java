public class Admin {
    public static void main(String[] args) throws Exception {
        String language = "java";
        String host = Client.env("NUVEXA_HOST", "127.0.0.1");
        int health = Integer.parseInt(Client.env("NUVEXA_HEALTH_PORT", "5762"));
        int management = Integer.parseInt(Client.env("NUVEXA_MANAGEMENT_PORT", "5763"));
        int https = Integer.parseInt(Client.env("NUVEXA_MANAGEMENT_HTTPS_PORT", "5764"));
        Client.http("GET", "http://" + host + ":" + health + "/health", null, false);
        Client.http("GET", "http://" + host + ":" + health + "/metrics", null, false);
        String base = "http://" + host + ":" + management;
        for (String path : new String[] { "/api/whoami", "/api/overview", "/api/connections", "/api/channels", "/api/streams", "/api/exchanges", "/api/queues", "/api/bindings", "/api/users", "/api/permissions", "/api/vhosts", "/api/policies" })
            Client.http("GET", base + path, null, true);
        String vhost = "vh-" + language;
        String account = "user-" + language;
        String policy = "policy-" + language;
        Client.http("PUT", base + "/api/vhosts/" + vhost, null, true);
        Client.http("PUT", base + "/api/users/" + account, "{\"password\":\"sample-pass\",\"tags\":[\"management\"]}", true);
        Client.http("PUT", base + "/api/permissions", "{\"user\":\"" + account + "\",\"vhost\":\"" + vhost + "\",\"configure\":\".*\",\"write\":\".*\",\"read\":\".*\"}", true);
        Client.http("PUT", base + "/api/policies/" + policy, "{\"vhost\":\"/\",\"pattern\":\"sample-.*\",\"priority\":1,\"messageTtlMs\":60000,\"maxLength\":100,\"deadLetterExchange\":\"\",\"deadLetterRoutingKey\":\"\"}", true);
        Client.http("DELETE", base + "/api/policies/" + policy + "?vhost=/", null, true);
        Client.http("DELETE", base + "/api/permissions?user=" + account + "&vhost=" + vhost, null, true);
        Client.http("DELETE", base + "/api/users/" + account, null, true);
        Client.http("DELETE", base + "/api/vhosts/" + vhost, null, true);
        Client.http("GET", "https://" + host + ":" + https + "/api/whoami", null, true);
    }
}
