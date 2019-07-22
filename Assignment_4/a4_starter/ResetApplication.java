import org.apache.kafka.common.serialization.Serdes;
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


public class ResetApplication {

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
	props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir);

        StreamsBuilder builder = new StreamsBuilder();

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

	streams.cleanUp();

	streams.close();
    }
}
