
Reza Marzban
__________________________________________
Kafka

Producer: creates and sent 500 random number in range of [0,5000] with topic kafka_Max.
Consumer: get the messages with Topic kafka_Max, and then calculate the max of all values and print it out.

How To Run Producer.java:
	javac -cp .:$CLASSPATH Producer.java
	java -cp .:$CLASSPATH Producer

How To Run Producer.java:
	javac -cp .:$CLASSPATH Consumer.java
	java -cp .:$CLASSPATH Consumer
	
The output would be printed out on the console!

Attached Files:
2 java source files, 2 output screenshots, 1 ReadMe txt file.


Sources used:
Kafka Documentation: https://kafka.apache.org/documentation/


