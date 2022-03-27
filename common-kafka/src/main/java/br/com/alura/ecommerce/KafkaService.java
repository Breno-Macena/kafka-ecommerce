package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, Message<T>> consumer;
    private final ConsumerFunction<T> parse;

    private KafkaService(String groupId, ConsumerFunction<T> parse, Class<T> type, Map<String, String> properties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(groupId, type, properties));
    }

    KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Class<T> type, Map<String, String> properties) {
        this(groupId, parse, type, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Class<T> type, Map<String, String> properties) {
        this(groupId, parse, type, properties);
        consumer.subscribe(topic);
    }

    @SuppressWarnings("InfiniteLoopStatement")
    void run() {
        while(true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if(! records.isEmpty()) {
                System.out.println("------------------------------------------------");
                System.out.println(new Timestamp((new Date()).getTime()) + " " + records.count() + " registers found.");
                for(var record : records) {
                    try {
                        this.parse.consume(record);
                    } catch (Exception e) {
                        // only catches exception becuase not matter which Exception I want to recover and parse
                        // the next one
                        // so far, just logging the exception for this message
                    }
                }
            }
        }
    }

    private Properties getProperties(String groupId, Class<T> type, Map<String, String> overrideProperties) {
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
        properties.putAll(overrideProperties);

        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
