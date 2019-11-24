package semantics;

import java.util.Optional;
import java.util.Properties;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

public class Producer {
	public static FlinkKafkaProducer<String> createStringProducer(final String topic,
            Properties prop) {
	 
	 Optional<FlinkKafkaPartitioner<String>> opt = Optional.empty();
	 
		return new FlinkKafkaProducer<String>( topic,  new KeyedSerializationSchema<String>() {

			
			public byte[] serializeKey(String element) {
				// TODO Auto-generated method stub
				return element.getBytes();
			}

			
			public byte[] serializeValue(String element) {
				// TODO Auto-generated method stub
				return element.getBytes();
			}

			
			public String getTargetTopic(String element) {
				// TODO Auto-generated method stub
				return topic;
			}
		},prop, opt, Semantic.EXACTLY_ONCE,5);
    }
}
