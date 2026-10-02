import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class Consume {
    public static void main(String[] args) throws Exception {
        String language = "java";
        String stream = "mailbox-" + language;
        String durable = "box-" + language;
        String tail = "tail-" + language;
        try (Client client = Client.connect(language)) {
            client.ensureStream(stream, new String[] { stream + ".>" }, 1, -1, -1, 0);
            client.ensureConsumer(stream, durable, stream + ".>", false, 0, 0);
            Client.Header[] plain = { new Client.Header("content-type", "text/plain".getBytes()) };
            client.publish(stream + ".created", "ack me".getBytes(), "", plain);
            client.publish(stream + ".created", "nack me".getBytes(), "", plain);
            var found = new HashMap<String, Client.Delivery>();
            for (var message : client.fetch(stream, durable, 32))
                found.put(new String(message.payload, StandardCharsets.UTF_8), message);
            client.ack(stream, durable, found.get("ack me"));
            System.out.println("ack");
            client.nack(stream, durable, found.get("nack me"));
            System.out.println("nack");
            Client.Delivery again = null;
            for (var message : client.fetch(stream, durable, 32))
                if (new String(message.payload, StandardCharsets.UTF_8).equals("nack me")) again = message;
            if (again == null) throw new IllegalStateException("nack was not redelivered");
            System.out.println("redelivered " + again.attempts);
            client.ack(stream, durable, again);
            client.reset(stream, durable, 0);
            var reset = client.fetch(stream, durable, 1);
            if (reset.isEmpty()) throw new IllegalStateException("reset did not return a message");
            System.out.println("after reset offset " + reset.get(0).offset);
            client.ack(stream, durable, reset.get(0));
            client.release(stream, tail);
            client.ensureConsumer(stream, tail, "", true, 1, 0);
            client.publish(stream + ".created", "tail me".getBytes(), "", new Client.Header[0]);
            Client.Delivery tailed = null;
            for (var message : client.fetch(stream, tail, 32))
                if (new String(message.payload, StandardCharsets.UTF_8).equals("tail me")) tailed = message;
            if (tailed == null) throw new IllegalStateException("tail message was not delivered");
            client.ack(stream, tail, tailed);
            client.release(stream, tail);
            client.ensureConsumer(stream, "from0-" + language, "", false, 2, 0);
            System.out.println("offset consumer from0-" + language);
        }
    }
}
