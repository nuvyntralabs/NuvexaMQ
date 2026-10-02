public class Ping {
    public static void main(String[] args) throws Exception {
        try (Client client = Client.connect("java")) {
            client.ping();
        }
    }
}
