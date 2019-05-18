
// Reza Marzban 
// Kafka

//Producer

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

//Our Main producer that create 500 records on topic kafka_Max.
//This producer creates and sent 500 random number in range of [0,5000].
public class Producer{
    public static void main(String[] args){
		//creating default properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
		//both key and value are using IntegerSerializer
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		//following properties make producer faster
		properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 5);
		properties.put(ProducerConfig.LINGER_MS_CONFIG, 1000);
		properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.put(ProducerConfig.ACKS_CONFIG, "0");
		//create a kafka Producer with our properties
        KafkaProducer Producer1 = new KafkaProducer(properties);
		System.out.println();
		System.out.println("______________________________________________________________");
		System.out.println("Random Numbers created by Producer:");
		ProducerRecord<Integer, Integer> record;
        //creates 500 records and send them in an Async way.
		for(int i = 0; i < 500; i++){
			int rnd=(int)(Math.random() * (5000));
			//the topic is kafka_Max, the key is the index from 1 to 500, the value is a integer between 1 to 5000.
			record=new ProducerRecord("kafka_Max", i+1, rnd);
			//Print the created random number
			System.out.print(rnd);
			System.out.print(", ");
			try{
				//send the record in an Async way.
				Producer1.send(record);
			}catch (Exception e){
				//throw exception if connection is corrupted
				e.printStackTrace();
			}
		}
		System.out.println(".");
		System.out.println("______________________________________________________________");
		System.out.println();
		//close the producer connection
		Producer1.close();
	}
}