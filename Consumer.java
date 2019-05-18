
// Reza Marzban 
// Kafka

//Consumer

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.ArrayList;
import java.util.Collections;



//Our Main Consumer gets the messages with Topic kafka_Max, and then calculate the max of all values and print it out.
public class Consumer{
    public static void main(String[] args) {
		//creating default properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
		//Group id of this consumer is Reza
		properties.put("group.id", "Reza");
		properties.put("enable.auto.commit", "true");
		properties.put("auto.commit.interval.ms", "1000");
		properties.put("session.timeout.ms", "30000");
		//both key and value are using IntegerDeserializer
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        //create a kafka Consumer with our properties
        KafkaConsumer<Integer,Integer> Consumer1 = new KafkaConsumer<Integer,Integer>(properties);
		//create an array of topics that our consumer will subscribe to.
        ArrayList ConsumerTopics = new ArrayList();
		//add kafka_Max to our ConsumerTopics
        ConsumerTopics.add("kafka_Max");
		//subscribe kafkaConsumer to all of topics
        Consumer1.subscribe(ConsumerTopics);
		ArrayList<Integer> values = new ArrayList();
		System.out.println();
		System.out.println("______________________________________________________________");
		int i=1;
        try{
            while (true){
				if(i>500){break;}
				//get 100 record each time
                ConsumerRecords<Integer, Integer> records = Consumer1.poll(100);
                for (ConsumerRecord<Integer, Integer> record: records){
					// add the value of each record to the arraylist
					values.add(record.value());
					i++;
                }
			}
        }catch (Exception e){
			//print the error
            System.out.println(e.getMessage());
        }finally {
			//get the maximum of all received values and print it out.
			Integer Maximum=Collections.max(values);
			System.out.print("The maximum value is: ");
			System.out.println(Maximum);
			System.out.println("______________________________________________________________");
			//close the consumer
            Consumer1.close();
        }
    }
}
