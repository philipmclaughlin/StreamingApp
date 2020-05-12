import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;

import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;

import org.apache.kafka.streams.kstream.Windowed;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

public final class TopPagesProcessor {
    
    public static final String USERS_TOPIC = "users";
    public static final String PAGEVIEWS_TOPIC = "pageviews";
    public static final String TOPPAGES_TOPIC = "phil_test";
    
    
    public static Map<String, Long> maleTop10 = new HashMap<String, Long>();
    
    static Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-joyn");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
    
    static void createTopPagesStream(final StreamsBuilder builder) {
    
        // A hopping time window with a size of 5 minutes and an advance interval of 1 minute.
        // The window's name -- the string parameter -- is used to e.g. name the backing state store.
        long windowSizeMs = TimeUnit.MINUTES.toMillis(1);
        long advanceMs =    TimeUnit.SECONDS.toMillis(10);
        TimeWindows.of(windowSizeMs).advanceBy(advanceMs);
    
        
        final KStream<String, String> users = builder.stream(USERS_TOPIC).map((key, value) -> KeyValue.pair(key.toString(), getJsonValueFromKey(value.toString(), "gender")));
        final KStream<String, String> pageviews = builder.stream(PAGEVIEWS_TOPIC).map((key, value) -> KeyValue.pair(getJsonValueFromKey(value.toString(), "userid"), getJsonValueFromKey(value.toString(), "pageid")));
    
    
        KStream<String, String>[] branches = users.join(pageviews,
                        (leftValue, rightValue) -> "{\"gender\":\"" + leftValue + "\",\"pageid\":\"" + rightValue + "\"}", /* ValueJoiner */
                        JoinWindows.of(TimeUnit.MINUTES.toMillis(10)),
                        Joined.with(
                                        Serdes.String(), /* key */
                                        Serdes.String(),   /* left value */
                                        Serdes.String())  /* right value */
                                                       ).map((key, value) -> KeyValue.pair(getJsonValueFromKey(value.toString(), "gender"),
                        getJsonValueFromKey(value.toString(), "pageid")))
                        .branch(
                        (key, value) -> key.equals("MALE"), /* first predicate  */
                        (key, value) -> key.equals("FEMALE"), /* second predicate */
                        (key, value) -> key.equals("OTHER"));          /* third predicate  */
    
    
        KStream<String, String> maleOutput = branches[0].groupBy((key, value) -> value).count().toStream().map((key, value) -> KeyValue.pair("MALE", updateMaleTopTen(key, value)));
   
  
        KStream<String, String> oneMinuteOutput = maleOutput.groupByKey().windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                        .aggregate(String::new, (key, value, aggr) -> { return value;}).toStream().map((key, value) -> KeyValue.pair("MALE", value));
        
        
        oneMinuteOutput.foreach(new ForeachAction<String, String>() {
            public void apply(String key, String value) {
                System.out.println(key + " : " + value);
            }
        });
    


    }
    
    private static String updateMaleTopTen(String page, Long count) {
        if(maleTop10.size()>=10){
            Long lowestCount = maleTop10.entrySet().iterator().next().getValue();
            String lowestPage = maleTop10.entrySet().iterator().next().getKey();
            if(count>lowestCount){
                maleTop10.remove(lowestPage);
                maleTop10.put(page, count);
                maleTop10 = sortMapByValue();
            }
        }else {
            maleTop10.put(page, count);
            maleTop10 = sortMapByValue();
        }
        return new JSONObject(maleTop10).toJSONString();
    }
    
    private static LinkedHashMap<String, Long> sortMapByValue() {
        return maleTop10
                        .entrySet()
                        .stream()
                        .sorted(comparingByValue())
                        .collect(toMap(e -> e.getKey(), e -> e.getValue(), (e1, e2) -> e2,
                                        LinkedHashMap::new));
    }
    
    private static String getJsonValueFromKey(String jsonString, String key) {
        JSONParser parser = new JSONParser();
        try {
            JSONObject json = (JSONObject) parser.parse(jsonString);
            return json.get(key).toString();
        } catch (ParseException e) {
            return "";
        }
    }
    
    public static void main(final String[] args) {
        final Properties props = getStreamsConfig();
        
        final StreamsBuilder builder = new StreamsBuilder();
        createTopPagesStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);
        
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
        
        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
