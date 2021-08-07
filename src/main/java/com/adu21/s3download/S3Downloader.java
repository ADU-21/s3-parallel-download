package com.adu21.s3download;

import java.io.File;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;

/**
 * @author LukeDu
 * @date 2021/8/7
 */
public class S3Downloader {

    private static final int TIMEOUT_MILLIS = 60 * 60 * 1000;

    public static void main(String[] args) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setConnectionTimeout(TIMEOUT_MILLIS);
        clientConfiguration.setSocketTimeout(TIMEOUT_MILLIS);
        clientConfiguration.setMaxConnections(500);
        clientConfiguration.setMaxErrorRetry(10);
        clientConfiguration.setProtocol(Protocol.HTTPS);

        AmazonS3 client = AmazonS3ClientBuilder.standard()
            .withRegion(Regions.US_EAST_1)
            .withClientConfiguration(clientConfiguration)
            .withCredentials(new EnvironmentVariableCredentialsProvider())
            .build();

        File tmpFile = new File("/tmp/test.json");

        long startTime = System.currentTimeMillis();
        client.getObject(new GetObjectRequest("duyidong-archive", "data/175M_test_data.json"), tmpFile);
        long endTime = System.currentTimeMillis();
        System.out.println("Run time: " + (endTime - startTime) + "ms");
    }
}
