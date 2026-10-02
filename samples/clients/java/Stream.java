public class Stream {
    public static void main(String[] args) throws Exception {
        String language = "java";
        String stream = "catalog-" + language;
        try (Client client = Client.connect(language)) {
            client.ensureStream(stream, new String[] { stream + ".>" }, 2, 86_400_000L, 1_048_576L, 65_536);
            byte[] body = "{\"id\":1}".getBytes();
            Client.Header[] headers = { new Client.Header("content-type", "application/json".getBytes()) };
            var keyed = client.publish(stream + ".created", body, "alpha", headers);
            var first = client.publish(stream + ".created", body, "", new Client.Header[0]);
            var second = client.publish(stream + ".created", body, "", new Client.Header[0]);
            if (keyed.isEmpty() || first.get(0).partition == second.get(0).partition)
                throw new IllegalStateException("round-robin did not use both partitions");
        }
    }
}
