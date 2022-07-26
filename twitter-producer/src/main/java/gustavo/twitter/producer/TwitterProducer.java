package gustavo.twitter.producer;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.Tweet;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class TwitterProducer {

    public static final String TWITTER_BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAB2gfAEAAAAA4FbtChFFpEgt5nm5ylGfp28qnyE%3DDd3c3BFqEVFdJGEuLFn1qOizfvzTBBfSmPvWZOixcYlIo6g6Fz";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public static void main(String... args) throws ApiException {
        new TwitterProducer().run();
    }

    public void run() throws ApiException {
        TwitterApi apiInstance = new TwitterApi(new TwitterCredentialsBearer(TWITTER_BEARER_TOKEN));
        List<Tweet> tweets = getTweets(apiInstance);
        processTweets(tweets);
    }

    private void processTweets(List<Tweet> tweets) {
        if (tweets == null || tweets.isEmpty()) {
            logger.info("No response");
            return;
        }

        try (KafkaProducer<String, String> producer = getProducer()) {
            sendMessages(tweets, producer);
        }
    }

    private void sendMessages(List<Tweet> tweets, KafkaProducer<String, String> producer) {
        tweets.forEach(tweet -> sendTweet(producer, tweet));
    }

    private void sendTweet(KafkaProducer<String, String> producer, Tweet tweet) {
        ProducerRecord<String, String> record = new ProducerRecord<>("twitter_tweets", tweet.getId(), tweet.getText());
        logger.info("Sending record: {}", record.toString());
        producer.send(record, (metadata, exception) -> {
            if(exception != null) {
                logger.error("Exception was thrown: ", exception);
            }
        });
    }

    @NotNull
    private KafkaProducer<String, String> getProducer() {
        var properties = new HashMap<String, Object>();
        // minimum needed
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // idempotence
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // configured by the property above, put here just as a sample
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // batching config
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.SNAPPY);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "20");

        return new KafkaProducer<>(properties);
    }

    @Nullable
    private List<Tweet> getTweets(TwitterApi apiInstance) throws ApiException {
        return apiInstance.tweets().tweetsRecentSearch("apache kafka")
                .maxResults(10)
                .tweetFields(Set.of("author_id", "geo", "source"))
                .execute()
                .getData();
    }
}
