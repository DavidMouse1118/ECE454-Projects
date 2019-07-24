import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;


public class A4Application {

	public static void main(String[] args) throws Exception {
		// do not modify the structure of the command line
		String bootstrapServers = args[0];
		String appName = args[1];
		String studentTopic = args[2];
		String classroomTopic = args[3];
		String outputTopic = args[4];
		String stateStoreDir = args[5];

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir);

		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> student = builder.stream(studentTopic);
		KStream<String, String> classroom = builder.stream(classroomTopic);
		
		// // Output: <key = student_id, value = classroom_id>
		KTable<String, String> studentToClassroom = student
			.groupByKey()
			.reduce((aggValue, newValue) -> newValue);

		// Output: <key = classroom_id, value = occupancy>
		KTable<String, Long> classroomToOccupancy = studentToClassroom
			.groupBy((studentId, roomId) ->  new KeyValue<>(roomId, studentId))
			.count();

		// Output: <Key = classroom_id, value = capacity>
		KTable<String, String> classroomToCapacity = classroom
			.groupByKey()
			.reduce((aggValue, newValue) ->  newValue);

		// Joining two tables classroomToOccupancy and classroomToCapacity
		// Output: <Key = classroom_id, value = "occupancy, capacity">
		KTable<String, String> classroomStatus = classroomToOccupancy
			.join(classroomToCapacity, (occupancy, capacity) -> {
				return occupancy.toString() + "," + capacity.toString();
			});

		// Gnerate the output table by comparing the current status and the previous status
		KTable<String, String> output = classroomStatus
			.toStream()
			.groupByKey()
			.aggregate(
				() -> null,
				(key, newValue, oldValue) -> {
					if (oldValue == null) {
						return newValue + "," + null;
					} 
					
					// System.out.println("newValue" + newValue);
					// System.out.println("oldValue" + oldValue);

					int newOccupancy = Integer.parseInt(newValue.split(",")[0]);
					int newCapacity = Integer.parseInt(newValue.split(",")[1]);

					// Exceed the current capacity, return the new occupancy
					if (newOccupancy > newCapacity) {
						return newValue + "," + newOccupancy;
					}

					int oldOccupancy = Integer.parseInt(oldValue.split(",")[0]);
					int oldCapacity = Integer.parseInt(oldValue.split(",")[1]);

					// Does not exceed the current capacity, check previous state
					// if previous state is not ok, return ok
					if (oldOccupancy > oldCapacity) {
						return newValue + "," + "OK"; 
					} else {
						return newValue + "," + null;
					}
				}
			);

		Serde<String> stringSerde = Serdes.String();

		output.toStream()
			.mapValues((value) -> value.split(",")[2])
			.filter((key, value) -> {
				return !value.equals("null");
			})
			.to(outputTopic, Produced.with(stringSerde, stringSerde));


		KafkaStreams streams = new KafkaStreams(builder.build(), props);

		// this line initiates processing
		streams.start();

		// shutdown hook for Ctrl+C
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
