package rabbit_kafka_bridge;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
//import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.producer.ProducerConfig;

 
public class KafkaController {
 
    public void sendMessage(String post){
    	
    	String msg = "";
//    	
              
        try {

        Random rnd  = new Random();
        
        //Integer key1 = rnd.nextInt(255);
        	
        KeyedMessage<String, String> data = new KeyedMessage<String, String>("disqus.stream.main.out", null,post);
        
        //Integer key2 = rnd.nextInt(255);
        
        KeyedMessage<String, String> dataII = new KeyedMessage<String, String>("disqus.fullfeed.in", null,post);
              
        DisqusProducer.kproducer.send(data);
        
        DisqusProducer.kproducer.send(dataII);
        
       // Thread.sleep(1);
        
        
        }
        catch (Exception e) {
  			// TODO Auto-generated catch block
  			e.printStackTrace();
  			//DisqusProducer.kproducer.close();
  	        DisqusProducer.logger.error(e.getMessage().toString());
  		}
        
        
    }


}