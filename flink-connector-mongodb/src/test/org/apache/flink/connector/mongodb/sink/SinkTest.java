package org.apache.flink.connector.mongodb.sink;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.connector.mongodb.common.utils.RedisHash;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkTest {

	public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<WordCount> counts = env.fromElements(
            new WordCount("hello", 1l),
            new WordCount("world", 2l),
            new WordCount("flink", 3l)
    );

		RedisSink<WordCount> sink = RedisSink.<WordCount>builder()
				 .setUri("redis://localhost:6379")
				 .setBatchSize(52)
				 .setSerializationSchema(
						 (wordcount, context) -> new RedisHash(wordcount.word, wordcount.toMap()))
				 .build();

		counts.sinkTo(sink);
		
    env.execute("Flink Cassandra Sink Example");
    
    // docker run -d --name my-redis -p 6379:6379 redis
    // docker exec -it my-redis redis-cli
    //  HGETALL flink
 	}
	
	static class WordCount {

		WordCount() {}
		WordCount(String word, long count) {
			this.word = word;
			this.count = count;
		}
		
		public String word;
		public long count;
		
    Map<String, String> toMap() {
    	Map<String, String> asMap = new HashMap<>();
    	asMap.put(word, word);
    	asMap.put(word, "" + count);
    	return asMap;
    }
	}  
}
