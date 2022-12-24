import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.BasicConfigurator;

import java.util.Random;

public class MainLogic {
    public static String BUCKET_PATH = "s3n://assignment2gy";

    public static void main(String[] args) {
        String randomId = RandomStringUtils.random(7, true, true);

        BasicConfigurator.configure();
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClient.builder().withRegion(Regions.US_EAST_1).build();

        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
                .withJar(BUCKET_PATH + "/Splitter.jar")
                .withMainClass("Splitter")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data",
                        BUCKET_PATH + "/Output");
        StepConfig stepConfig1 = new StepConfig()
                .withName("Splitter")
                .withHadoopJarStep(hadoopJarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
                .withJar(BUCKET_PATH + "/NrTrCalculator.jar")
                .withMainClass("NrTrCalculator")
                .withArgs(new String[] {randomId});
        StepConfig stepConfig2 = new StepConfig()
                .withName("NrTrCalculator")
                .withHadoopJarStep(hadoopJarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
                .withJar(BUCKET_PATH + "/Joiner.jar")
                .withMainClass("Joiner")
                .withArgs(new String[] {randomId});
        StepConfig stepConfig3 = new StepConfig()
                .withName("Joiner")
                .withHadoopJarStep(hadoopJarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
                .withJar(BUCKET_PATH + "/DEprobability.jar")
                .withMainClass("DEprobability")
                .withArgs(new String[] {randomId});
        StepConfig stepConfig4 = new StepConfig()
                .withName("DEprobability")
                .withHadoopJarStep(hadoopJarStep4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStep5 = new HadoopJarStepConfig()
                .withJar(BUCKET_PATH + "/SortOutput.jar")
                .withMainClass("SortOutput")
                .withArgs(new String[] {randomId});
        StepConfig stepConfig5 = new StepConfig()
                .withName("SortOutput")
                .withHadoopJarStep(hadoopJarStep5)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(5)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName("Assignment2-Key-Pair")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("KnowledgeBaseForWordPrediction")
                .withReleaseLabel("emr-5.20.0")
                .withInstances(instances)
                .withSteps(stepConfig1, stepConfig2, stepConfig3, stepConfig4, stepConfig5)
                .withLogUri(BUCKET_PATH + randomId + "/logs/")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId + "and random id: " + randomId);
    }
}
