package semantics;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;





public class Consumer {

	public void listenToDispatcher(StreamExecutionEnvironment env) {
		Properties prop = new Properties();
		prop.setProperty("bootstrap.servers", "localhost:9092");
		prop.setProperty("transaction.timeout.ms", "60000");
	    FlinkKafkaProducer<String> producer = Producer.createStringProducer( "test-producer", prop);
	    FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<String>("test-consumer", new KafkaCustom(),
                        prop);
	    

	    env.addSource(consumer).map( new MapFunction<String, String>() {

			public String map(String value) throws Exception {
				return "Appender:" +value ;
			}
		}).addSink(producer);
	}

}
