package br.com.alura.ecommerce.consumer;

import br.com.alura.ecommerce.Message;
import br.com.alura.ecommerce.dispatcher.GsonSerializer;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, Message<T>> consumer;
    private final ConsumerFunction<T> parse;

    private KafkaService(String groupId, ConsumerFunction<T> parse, Map<String, String> properties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(groupId, properties));
    }

    public KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Map<String, String> properties) {
        this(groupId, parse, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Map<String, String> properties) {
        this(groupId, parse, properties);
        consumer.subscribe(topic);
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public void run() throws ExecutionException, InterruptedException {
        try(var deadLetter = new KafkaDispatcher<>()) {
            while(true) {
                var records = consumer.poll(Duration.ofMillis(100));
                if(! records.isEmpty()) {
                    System.out.println("------------------------------------------------");
                    System.out.println(new Timestamp((new Date()).getTime()) + " " + records.count() + " registers found.");
                    for(var record : records) {
                        try {
                            this.parse.consume(record);
                        } catch (Exception e) {
                            e.printStackTrace();
                            var message = record.value();
                            deadLetter.send("ECOMMERCE_DEADLETTER", message.getId().toString(),
                                    message.getId().continueWith("DeadLetter"),
                                    new GsonSerializer<>().serialize("", message));
                        }
                    }
                }
            }
        }
    }

    private Properties getProperties(String groupId, Map<String, String> overrideProperties) {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        // é necessário dizer qual o ID do grupo que está consumindo a mensagem, ou seja,
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // pode-se definir um identificador de cliente para esse consumidor, que será setado no kafka quando houver a atribuição da partição
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        // Se houver um rebalanceamento das partições no meio de um processamento de mensagens, isso vai gerar um erro de commit
        // após o consumo. E a informação de consumo daquelas mensagens se perderá. Para "suavizar" esse problema, podemos
        // definir o máximo de registros no poll como um, de forma que uma mensagem será processada por vez, e para
        // cada mensagem teremos um commit. Essa é uma configuração muito comum, e usada basteante inclusive em grandes empresas
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        // Pode acontecer do offset das mensagens ter sido "perdido". Isso ocorre, por exemplo, quando os dados são deletados.
        // Nesse caso, podemos configurar o reset do offset para começar de onde for mais propício. Por padrão, começa do último
        // offset, e isso pode culminar na perda de mensagens. Se começar do início, pode ocorrer de reprocessar uma mensagem
        // que já foi processada, porém, não houve o commit
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.putAll(overrideProperties);

        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
