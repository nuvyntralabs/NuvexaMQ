public class Exchange {
    public static void main(String[] args) throws Exception {
        String language = "java";
        String queue = "work-" + language;
        String dead = "dead-" + language;
        String direct = "direct-" + language;
        String fanout = "fanout-" + language;
        String topic = "topic-" + language;
        String headers = "headers-" + language;
        try (Client client = Client.connect(language)) {
            client.declareExchange("amq.direct", "direct");
            client.declareExchange(direct, "direct");
            client.declareExchange(fanout, "fanout");
            client.declareExchange(topic, "topic");
            client.declareExchange(headers, "headers");
            client.declareQueue(dead, -1, -1, "", "");
            client.declareQueue(queue, 60000, 100, direct, "expired");
            client.bind(direct, dead, "expired", new String[0][]);
            client.bind(direct, queue, "work.created", new String[0][]);
            client.bind(fanout, queue, "", new String[0][]);
            client.bind(topic, queue, "work.*", new String[0][]);
            client.bind(headers, queue, "", new String[][] { { "format", "json" }, { "x-match", "all" } });
            byte[] body = "routed".getBytes();
            int count = 0;
            count += client.publishExchange(direct, "work.created", body, "order-1", new Client.Header[0]).size();
            count += client.publishExchange(fanout, "", body, "", new Client.Header[0]).size();
            count += client.publishExchange(topic, "work.created", body, "", new Client.Header[0]).size();
            count += client.publishExchange(headers, "", body, "", new Client.Header[] { new Client.Header("format", "json".getBytes()) }).size();
            count += client.publishExchange("", queue, body, "", new Client.Header[0]).size();
            if (count < 5) throw new IllegalStateException("expected at least 5 receipts, got " + count);
            client.purge(queue);
            client.deleteQueue(queue);
            client.deleteQueue(dead);
            for (String name : new String[] { direct, fanout, topic, headers }) client.deleteExchange(name);
        }
    }
}
