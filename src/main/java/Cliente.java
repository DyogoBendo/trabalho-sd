import com.rabbitmq.client.*;
import org.jgroups.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Cliente implements Receiver {
    private static final String EXCHANGE_NAME = "exchange_twink";
    private String meuUsuario;
    private JChannel jChannel; // Canal de comunicação em grupo

    public Cliente(String meuUsuario) {
        this.meuUsuario = meuUsuario;
    }

    static {
        System.setProperty("jgroups.fd_sock2.port_range", "100");
    }

    // --- MÉTODOS DO JGROUPS (CHAT PRIVADO) ---

    @Override
    public void viewAccepted(View new_view) {
        System.out.println("\n[SISTEMA DE GRUPO] Membros online (" + new_view.size() + "): " + new_view.getMembers());
        System.out.print("Escolha uma opção: "); // Tenta restaurar o prompt visualmente
    }

    @Override
    public void receive(Message msg) {
        System.out.println("\n[GRUPO - " + msg.getSrc() + "] diz: " + msg.getObject());
        System.out.print("Escolha uma opção: ");
    }

    // --- MÉTODOS DE CONTROLE DO MENU ---

    public void iniciar() throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        // 1. Configura a Conexão Base do RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel rabbitChannel = connection.createChannel();

        rabbitChannel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        String filaInscricoes = rabbitChannel.queueDeclare().getQueue();

        // 2. Configura a recepção assíncrona do Feed (Pub/Sub)
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String mensagem = new String(delivery.getBody(), "UTF-8");
            String autor = delivery.getEnvelope().getRoutingKey().replace("twink.", "");
            System.out.println("\n[FEED - @" + autor + "] " + mensagem);
            System.out.print("Escolha uma opção: ");
        };
        rabbitChannel.basicConsume(filaInscricoes, true, deliverCallback, consumerTag -> { });

        // 3. Loop do Menu Principal
        while (true) {
            exibirMenu();
            String opcao = reader.readLine();

            switch (opcao) {
                case "1": // Seguir alguém
                    System.out.print("Digite o usuário que deseja seguir: ");
                    String seguir = reader.readLine();
                    if (!seguir.isEmpty()) {
                        rabbitChannel.queueBind(filaInscricoes, EXCHANGE_NAME, "twink." + seguir);
                        System.out.println("-> Você começou a seguir @" + seguir);
                    }
                    break;

                case "2": // Fazer Postagem no Feed
                    System.out.print("O que você está pensando? ");
                    String twink = reader.readLine();
                    String routingKey = "twink." + meuUsuario;
                    rabbitChannel.basicPublish(EXCHANGE_NAME, routingKey, null, twink.getBytes("UTF-8"));
                    System.out.println("-> Postagem enviada para o feed público.");
                    break;

                case "3": // Entrar em um Grupo
                    System.out.print("Digite o nome do grupo privado para entrar: ");
                    String nomeGrupo = reader.readLine();

                    if (jChannel != null && jChannel.isConnected()) {
                        System.out.println("-> Você saiu do grupo anterior.");
                        jChannel.disconnect();
                    }

                    jChannel = new JChannel();
                    jChannel.name(meuUsuario);
                    jChannel.setReceiver(this);
                    jChannel.connect(nomeGrupo);
                    break;

                case "4": // Mandar mensagem no Grupo
                    if (jChannel == null || !jChannel.isConnected()) {
                        System.out.println("-> Erro: Você precisa entrar em um grupo primeiro (Opção 3).");
                    } else {
                        System.out.print("Digite a mensagem para o grupo: ");
                        String msgGrupo = reader.readLine();
                        jChannel.send(new ObjectMessage(null, msgGrupo)); // null = manda para todos do grupo
                    }
                    break;

                case "5": // Sair
                    System.out.println("Encerrando aplicação...");
                    if (jChannel != null) jChannel.close();
                    connection.close();
                    System.exit(0);
                    break;

                default:
                    System.out.println("-> Opção inválida.");
            }
        }
    }

    private void exibirMenu() {
        System.out.println("\n=== MENU (" + meuUsuario + ") ===");
        System.out.println("1 - Seguir um usuário");
        System.out.println("2 - Fazer uma postagem (Feed)");
        System.out.println("3 - Entrar em um Grupo Privado");
        System.out.println("4 - Enviar mensagem no Grupo Privado");
        System.out.println("5 - Sair");
        System.out.print("Escolha uma opção: ");
    }

    public static void main(String[] args) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        System.out.print("Digite seu nome de usuário: ");
        String usuario = in.readLine();

        Cliente cliente = new Cliente(usuario);
        cliente.iniciar();
    }
}