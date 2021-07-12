package pubsub;

// [START pubsub_quickstart_publisher]
// [START pubsub_publish]

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class PublisherExample {
    public static void main(String... argv) throws Exception {
        // TODO(developer): Replace these variables before running the sample.
        String projectId = "starlit-channel-317607";
        String topicId = "airlines";

        publisherExample(projectId, topicId, argv[0]);
        System.out.println("Done and dusted");
    }

    public static void publisherExample(String projectId, String topicId,String argv)
            throws IOException, ExecutionException, InterruptedException {
        TopicName topicName = TopicName.of(projectId, topicId);

        Publisher publisher = null;
        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();

            //String message = "Hello World!";

            //File file = new File("D:\\FromAsusLaptop\\C-Drive\\SparkKafka\\data\\fakefriends-noheader.csv");
            //
            //File file = new File("D:\\ideaprojall\\data\\book.txt");

            //File file = new File("D:\\ideaprojall\\data\\airlines2008.csv");
            System.out.println("File name ==>"+argv);
            File file = new File(argv);
            int count = 0;
            FileReader fr = new FileReader(file);   //reads the file
            BufferedReader br = new BufferedReader(fr);  //creates a buffering character input stream
            String line;
            while ((line = br.readLine()) != null) {
                ByteString data = ByteString.copyFromUtf8(line);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
                ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
                String messageId = messageIdFuture.get();

                if(count % 99 == 0 ) {
                    System.out.println("Published message ID: " + messageId);
                }
                count++;
            }
            fr.close();    //closes the stream and release the resources
            // Once published, returns a server-assigned message id (unique within the topic)

        } catch (Exception e){
            System.out.println(e.getMessage());

        }finally {
            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
    }
}