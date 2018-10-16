package ISS;

import java.util.List;
import java.util.Map;

//import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.log4j.Logger;

/**
 * Created by zhangbo on 10/05.
 */
public class JavaKafkaProducerPartitioner implements Partitioner {

    private Logger logger = Logger.getLogger(JavaKafkaProducerPartitioner.class);
    public JavaKafkaProducerPartitioner() {
        // TODO Auto-generated constructor stub
    }

    //@Override
    public void configure(Map<String, ?> configs) {
        // TODO Auto-generated method stub
    }

    //@Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // TODO Auto-generated method stub
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        int partitionNum = 0;
        partitionNum = Integer.valueOf(((String) key).replaceAll("key_", "").trim());
//        try {
//            partitionNum = Integer.parseInt (((GenericRecord)value).get("f1").toString());
//        } catch (Exception e) {
//            partitionNum = ((GenericRecord)value).get("f1").hashCode() ;
//        }
        logger.info("the message sendTo topic:"+ topic+" and the partitionNum:"+ partitionNum);
        //System.out.println(Math.abs(partitionNum  % numPartitions));
        return Math.abs(partitionNum  % numPartitions);
    }



    //@Override
    public void close() {
        // TODO Auto-generated method stub

    }

}