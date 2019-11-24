package semantics;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



public class InitJob {
	
	public static void main(String[] args) {
		
		   Consumer kafkaConsumer = new Consumer();
		    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		    CheckPoint.initialize(env);
		    kafkaConsumer.listenToDispatcher(env);
		    try {
		        env.execute("ESPRO-AGGREGATOR");
		    } catch (Exception e) {
		    }
	}
	
	
   

}
