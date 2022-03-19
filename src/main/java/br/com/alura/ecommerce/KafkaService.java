package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

class KafkaService implements Closeable {
    private final KafkaConsumer<String, String> consumer;
    private final ConsumerFunction parse;

    private KafkaService(String groupId, ConsumerFunction parse) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(properties(groupId));
    }

    KafkaService(String groupId, String topic, ConsumerFunction parse) {
        this(groupId, parse);
        consumer.subscribe(Collections.singletonList(topic));
    }

    KafkaService(String groupId, Pattern topic, ConsumerFunction parse) {
        this(groupId, parse);
        consumer.subscribe(topic);
    }

    void run() {
        while(true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if(! records.isEmpty()) {
                System.out.println("------------------------------------------------");
                System.out.println(new Timestamp((new Date()).getTime()) + " " + records.count() + " registers found.");
                for(var record : records) {
                    this.parse.consume(record);
                }
            }
        }
    }

    private static Properties properties(String groupId) {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // é necessário dizer qual o ID do grupo que está consumindo a mensagem, ou seja,
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // pode-se definir um identificador de cliente para esse consumidor, que será setado no kafka quando houver a atribuição da partição
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        // Se houver um rebalanceamento das partições no meio de um processamento de mensagens, isso vai gerar um erro de commit
        // após o consumo. E a informação de consumo daquelas mensagens se perderá. Para "suavizar" esse problema, podemos
        // definir o máximo de registros no poll como um, de forma que uma mensagem será processada por vez, e para
        // cada mensagem teremos um commit. Essa é uma configuração muito comum, e usada basteante inclusive em grandes empresas
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
