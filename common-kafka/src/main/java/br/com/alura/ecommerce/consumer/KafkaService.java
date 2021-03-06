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
        // ?? necess??rio dizer qual o ID do grupo que est?? consumindo a mensagem, ou seja,
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // pode-se definir um identificador de cliente para esse consumidor, que ser?? setado no kafka quando houver a atribui????o da parti????o
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        // Se houver um rebalanceamento das parti????es no meio de um processamento de mensagens, isso vai gerar um erro de commit
        // ap??s o consumo. E a informa????o de consumo daquelas mensagens se perder??. Para "suavizar" esse problema, podemos
        // definir o m??ximo de registros no poll como um, de forma que uma mensagem ser?? processada por vez, e para
        // cada mensagem teremos um commit. Essa ?? uma configura????o muito comum, e usada basteante inclusive em grandes empresas
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        // Pode acontecer do offset das mensagens ter sido "perdido". Isso ocorre, por exemplo, quando os dados s??o deletados.
        // Nesse caso, podemos configurar o reset do offset para come??ar de onde for mais prop??cio. Por padr??o, come??a do ??ltimo
        // offset, e isso pode culminar na perda de mensagens. Se come??ar do in??cio, pode ocorrer de reprocessar uma mensagem
        // que j?? foi processada, por??m, n??o houve o commit
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.putAll(overrideProperties);

        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
