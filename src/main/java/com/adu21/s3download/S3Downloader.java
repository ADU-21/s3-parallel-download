package com.adu21.s3download;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import org.apache.commons.io.IOUtils;

/**
 * @author LukeDu
 * @date 2021/8/7
 */
public class S3Downloader {
    private static final int TIMEOUT_MILLIS = 60 * 60 * 1000;
    private static final int NUMBER_OF_PARTS = 5;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bucketName = "duyidong-archive";
        String key = "data/175M_test_data.json";
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setConnectionTimeout(TIMEOUT_MILLIS);
        clientConfiguration.setSocketTimeout(TIMEOUT_MILLIS);
        clientConfiguration.setMaxConnections(500);
        clientConfiguration.setMaxErrorRetry(10);
        clientConfiguration.setProtocol(Protocol.HTTP);

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withRegion(Regions.US_EAST_1)
            .withClientConfiguration(clientConfiguration)
            .withCredentials(new EnvironmentVariableCredentialsProvider())
            .build();

        long startTime = System.currentTimeMillis();

        final List<GetObjectRequest> getPartRequests = prepareGetPartRequests(bucketName, key, s3Client);
        final List<Future<Chunk>> futures = submitToThreadPool(executorService, s3Client, getPartRequests);
        final ArrayList<Chunk> chunks = getOrderedChunks(futures);
        final String finalStr = processHeadAndTail(chunks);

        long endTime = System.currentTimeMillis();

        System.out.println(finalStr.length());
        System.out.println("Run time: " + (endTime - startTime) + "ms");

        executorService.shutdownNow();
    }

    private static String processHeadAndTail(ArrayList<Chunk> chunks) {
        final StringBuilder stringBuilder = new StringBuilder();
        chunks.forEach(chunk -> {
            stringBuilder.append(chunk.getHead());
        });
        // Process rest data which are head and tail in the chunk here
        return stringBuilder.toString();
    }

    private static ArrayList<Chunk> getOrderedChunks(List<Future<Chunk>> futures)
        throws InterruptedException, ExecutionException {
        final ArrayList<Chunk> chunks = new ArrayList<>();
        for (Future<Chunk> future : futures) {
            chunks.add(future.get());
        }
        chunks.sort(Comparator.comparing(Chunk::getIndex));
        return chunks;
    }

    private static List<Future<Chunk>> submitToThreadPool(ExecutorService executorService, AmazonS3 s3Client,
        List<GetObjectRequest> getPartRequests) {
        return IntStream.range(0, NUMBER_OF_PARTS).mapToObj(index -> {
            final GetObjectRequest getPartRequest = getPartRequests.get(index);
            return executorService.submit(() -> {
                String header;
                try (InputStream inputStream = s3Client.getObject(getPartRequest).getObjectContent()) {
                    // Process majority data in the chunk, return head and tail which are not complete data
                    header = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                }
                return new Chunk(header, "", index);
            });
        }).collect(Collectors.toList());
    }

    private static List<GetObjectRequest> prepareGetPartRequests(String bucketName, String key, AmazonS3 s3Client) {
        long size = s3Client.getObjectMetadata(bucketName, key)
            .getContentLength();
        long chunkSize = size / NUMBER_OF_PARTS;
        return IntStream.range(0, NUMBER_OF_PARTS)
            .mapToObj(index -> new AbstractMap.SimpleEntry<>(index * chunkSize,
                (index + 1) * chunkSize > size ? size - 1 : (index + 1) * chunkSize - 1))
            .map(range -> new GetObjectRequest(bucketName, key).withRange(range.getKey(), range.getValue()))
            .collect(Collectors.toList());
    }
}