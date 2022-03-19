package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

public class FraudDetectorService {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));
        while(true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if(! records.isEmpty()) {
                System.out.println("------------------------------------------------");
                System.out.println(new Timestamp((new Date()).getTime()) + " " + records.count() + " registers found.");
                for(var record : records) {
                    System.out.println("------------------------------------------------");
                    System.out.println("Processing new order, checking for fraud");
                    System.out.println(record.key());
                    System.out.println(record.value());
                    System.out.println(record.partition());
                    System.out.println(record.offset());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Order precessed");
                }
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // é necessário dizer qual o ID do grupo que está consumindo a mensagem, ou seja,
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());
        // pode-se definir um identificador de cliente para esse consumidor, que será setado no kafka quando houver a atribuição da partição
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, FraudDetectorService.class.getSimpleName() + "_" + UUID.randomUUID().toString());
        // Se houver um rebalanceamento das partições no meio de um processamento de mensagens, isso vai gerar um erro de commit
        // após o consumo. E a informação de consumo daquelas mensagens se perderá. Para "suavizar" esse problema, podemos
        // definir o máximo de registros no poll como um, de forma que uma mensagem será processada por vez, e para
        // cada mensagem teremos um commit. Essa é uma configuração muito comum, e usada basteante inclusive em grandes empresas
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }
}
