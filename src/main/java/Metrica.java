import com.rabbitmq.client.*;

public class Metrica {
    private static final String EXCHANGE_NAME = "exchange_twink";
    private static final String FILA_WORKERS = "fila_metricas_analise";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        // Declara uma fila durável para os workers
        channel.queueDeclare(FILA_WORKERS, true, false, false, null);

        // Bind com wildcard para receber todos os twinks de todos os usuários
        channel.queueBind(FILA_WORKERS, EXCHANGE_NAME, "twink.#");

        // Impede que o RabbitMQ envie mais de 1 mensagem por vez para o worker (Fair Dispatch)
        channel.basicQos(1);

        System.out.println(" [*] Worker aguardando mensagens para análise de métricas...");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String mensagem = new String(delivery.getBody(), "UTF-8");
            String autor = delivery.getEnvelope().getRoutingKey().replace("tweet.", "");

            // Simula o processamento da métrica
            int contagemPalavras = mensagem.split("\\s+").length;
            System.out.println(" [Análise] Twink de @" + autor + " tem " + contagemPalavras + " palavra(s).");

            // Confirma o processamento para a fila
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        // Auto-ack em false para garantir que a mensagem não se perca se o worker falhar
        channel.basicConsume(FILA_WORKERS, false, deliverCallback, consumerTag -> { });
    }
}