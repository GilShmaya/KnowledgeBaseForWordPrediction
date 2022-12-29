package utils;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import org.apache.log4j.BasicConfigurator;

public class StopWords {
    private static final String bucket = "assignment2gy";
    private static final String fileName = "eng-stopwords.txt";
    private Set<String> stopWordsSet;

    public static StopWords getInstance() {
        return StopWords.singletonHolder.instance;
    }

    private StopWords() {
        BasicConfigurator.configure();
        AmazonS3 s3 = (AmazonS3)((AmazonS3ClientBuilder)AmazonS3Client.builder().withRegion(Regions.US_EAST_1)).build();
        S3Object s3Object = s3.getObject(bucket, fileName);
        InputStream objectContent = s3Object.getObjectContent();
        Scanner scanner = (new Scanner(objectContent)).useDelimiter("\\A");
        String text = scanner.hasNext() ? scanner.next() : "";
        String[] wordsArray = text.split("\\s+");
        List<String> wordsList = Arrays.asList(wordsArray);
        this.stopWordsSet = new HashSet(wordsList);
    }

    public Set<String> getStopWordsSet() {
        return this.stopWordsSet;
    }

    private static class singletonHolder {
        private static final StopWords instance = new StopWords();

        private singletonHolder() {
        }
    }
}
