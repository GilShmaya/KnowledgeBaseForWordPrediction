package utils;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import org.apache.log4j.BasicConfigurator;

public class CounterN {
    private static AmazonS3 s3;
    private static final String bucket = "assignment2gy";
    private static final String fileName = "N.txt";

    public static CounterN getInstance() {
        return CounterN.singletonHolder.instance;
    }

    private CounterN() {
        BasicConfigurator.configure();
        s3 = (AmazonS3)((AmazonS3ClientBuilder)AmazonS3Client.builder().withRegion(Regions.US_EAST_1)).build();
    }

    public long getN() throws Exception {
        S3Object s3Object = s3.getObject(bucket, fileName);
        InputStream objectContent = s3Object.getObjectContent();
        Scanner scanner = (new Scanner(objectContent)).useDelimiter("\\A");
        String text = scanner.hasNext() ? scanner.next() : "";
        String[] testArray = text.split("\\s+");
        if (testArray.length > 1) {
            throw new Exception("Expected to find only the value of N.");
        } else {
            return Long.parseLong(testArray[0]);
        }
    }

    public void setN(long N) {
        String longValue = Long.toString(N);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(longValue.getBytes(StandardCharsets.UTF_8));
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength((long)longValue.length());
        PutObjectRequest request = new PutObjectRequest(bucket, fileName, inputStream, metadata);
        s3.putObject(request);
    }

    private static class singletonHolder {
        private static final CounterN instance = new CounterN();

        private singletonHolder() {
        }
    }
}
