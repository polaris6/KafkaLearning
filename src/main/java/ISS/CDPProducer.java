package ISS;

//import org.apache.avro.Schema;
//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ThreadLocalRandom;

/**
 * chronic-disease-project ������Ŀ��producer
 * Created by zhangbo on 2017/10/05
 */
public class CDPProducer {
    private Logger logger = Logger.getLogger(CDPProducer.class);
    public static final String TOPIC_NAME = "test";

    public static void main(String[] args) {
      
        String brokerList = "192.168.222.5:9092";
        String schemaRegistryUrl = "http://192.168.222.5:8081";

        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("request.required.acks", "0");
        props.put("producer.type", "async");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        /**
         * ���÷�����
         * ����key�������ݷ���
         * Ĭ���ǣ�kafka.producer.DefaultPartitioner ==> ����key��hash���з���
         * ��ѡ:kafka.serializer.ByteArrayPartitioner ==> ת��Ϊ�ֽ���������hash����
         */
        props.put("partitioner.class", "ISS.ProducerPartitioner");

        // ���Դ���
        props.put("message.send.max.retries", "3");

        // �첽�ύ��ʱ��(async)�������ύ�ļ�¼��
        props.put("batch.num.messages", "200");

        // ���û�������С��Ĭ��10KB
        props.put("send.buffer.bytes", "102400");

        // 3. ����Producer����
        //final KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);
        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 4. �������ݵ��������������̷߳���
        final AtomicBoolean flag = new AtomicBoolean(true);
        int numThreads = 500;
        
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            pool.submit(new Thread(new Runnable() {
                @Override
                public void run() {
                    while (flag.get()) {
                        // ��������
                        ProducerRecord message = null;
                        try {
                            message = generateProducerRecord();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        producer.send(message);
                        System.out.println("��������:" + message);

                        // ����һ��
                        try {
                            int least = 10;
                            int bound = 100;
                            Thread.sleep(ThreadLocalRandom.current().nextInt(least, bound));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    System.out.println(Thread.currentThread().getName() + " shutdown....");
                }
            }, "Thread-" + i));

        }

        // 5. �ȴ�ִ�����
        long sleepMillis = 600000;
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        flag.set(false);

        // 6. �ر���Դ

        pool.shutdown();
        try {
            pool.awaitTermination(6, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        } finally {
            producer.close(); // ���֮�����
        }
    }

    /**
     * ��ȡ��Ϣ
     */
    private static ProducerRecord<String, String> generateProducerRecord() throws IOException {
        String key = "key_" + Math.random() * 5;
        StringBuilder sb = new StringBuilder();
        sb.append(Math.random() * 10).append(",").append(Math.random() * 10).append(",").append(Math.random() * 10);
        String message = sb.toString().trim();
        return new ProducerRecord(TOPIC_NAME, key, message);
    }



/*    private static ProducerRecord<String, GenericRecord> generateProducerRecord() throws IOException {

        String key = "user1";
        String userSchema = "{\"type\":\"record\"," +
                "\"name\":\"Data\"," +
                "\"fields\":[{\"name\":\"f1\",\"type\":\"long\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        GenericRecord message = new GenericData.Record(schema);
*//*        final GenericRecordBuilder message =
                new GenericRecordBuilder(loadSchema("data.avsc"));*//*

        message.put("f1", System.currentTimeMillis());
        //System.out.println(message);
        return new ProducerRecord(TOPIC_NAME, key, message);
    }

    private static Schema loadSchema(final String name) throws IOException {
        try (InputStream input = CDPProducer.class.getClassLoader()
                .getResourceAsStream("avro/io/confluent/examples/streams/" + name)) {
            return new Schema.Parser().parse(input);
        }
    }*/
}