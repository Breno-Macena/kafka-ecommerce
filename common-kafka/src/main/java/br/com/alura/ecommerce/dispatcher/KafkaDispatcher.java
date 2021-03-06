package br.com.alura.ecommerce.dispatcher;

import br.com.alura.ecommerce.CorrelationId;
import br.com.alura.ecommerce.Message;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaDispatcher<T> implements Closeable {
    private final KafkaProducer<String, Message<T>> producer;

    public KafkaDispatcher(){
        this.producer = new KafkaProducer<>(properties());
    }

    private static Properties properties() {
        var properties = new Properties();
        // onde o kafka está rodando
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // tanto a chave quanto o valor vão transformar a mensagem e a chave baseada em string (string para byte)
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        // Existem situações em que uma máquina líder recebe uma mensagem e não consegue enviar para as máquinas réplicas.
        // Por exemplo, se a máquina líder cair assim que receber a mensagem, ou a máquina líder estiver sozinha no ar e
        // cair antes das réplicas subirem.
        // Isso pode causar a perda de mensagens.
        // Para maior confiança (reliability), podemos configurar o produtor para obrigar que o kafka só retorne OK
        // se o líder conseguir mandar a mensagem para as réplicas.
        // Isso pode causar lentidão, mas ao menos é mais seguro.
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    public void send(String topic, String key, CorrelationId id, T payload) throws ExecutionException, InterruptedException {
        var future = sendAsync(topic, key, id, payload);
        future.get();
    }

    public Future<RecordMetadata> sendAsync(String topic, String key, CorrelationId id, T payload) {
        var message = new Message<>(id.continueWith("_" + topic), payload);
        var record = new ProducerRecord<>(topic, key, message);

        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucesso ao enviar em " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };

        return producer.send(record, callback);
    }

    @Override
    public void close() {
        producer.close();
    }
}
