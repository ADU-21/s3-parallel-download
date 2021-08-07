package com.adu21.s3download;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

/**
 * @author LukeDu
 * @date 2021/8/7
 */
public class S3Downloader {
    private static final int TIMEOUT_MILLIS = 60 * 60 * 1000;

    public static void main(String[] args) throws IOException {

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

        S3StreamReader s3StreamReader = new S3StreamReader(client, 5 * 1024 * 1024);
        long startTime = System.currentTimeMillis();
        InputStream inputStream = s3StreamReader.get("duyidong-archive", "data/22M_test_data.json");
        long endTime = System.currentTimeMillis();
        System.out.println("Run time: " + (endTime - startTime) + "ms");

        Files.copy(inputStream, new File("/tmp/test.json").toPath(), StandardCopyOption.REPLACE_EXISTING);
        inputStream.close();
    }
}