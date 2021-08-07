package com.adu21.s3download;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Enumeration;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;

/**
 * @author LukeDu
 * @date 2021/8/7
 */
public class S3StreamReader {
    private final AmazonS3 s3Client;
    private final long bufferSize;

    public S3StreamReader(AmazonS3 s3Client, long bufferSize) {
        this.s3Client = s3Client;
        this.bufferSize = bufferSize;
    }

    public InputStream get(String bucketName, String key) {
        long totalSize = getSize(bucketName, key);

        Enumeration<S3ObjectInputStream> s3Enumeration = getEnumeration(bucketName, key, totalSize);
        Enumeration<? extends InputStream> bufferedEnumeration = getBufferedEnumeration(s3Enumeration);

        return new SequenceInputStream(bufferedEnumeration);
    }

    private long getSize(String bucketName, String key) {
        return s3Client
            .getObjectMetadata(bucketName, key)
            .getContentLength();
    }

    private Enumeration<S3ObjectInputStream> getEnumeration(String bucketName, String key, long totalSize) {
        return new Enumeration<S3ObjectInputStream>() {
            Long currentPosition = 0L;

            @Override
            public boolean hasMoreElements() {
                return currentPosition < totalSize;
            }

            @Override
            public S3ObjectInputStream nextElement() {
                // The Range request is inclusive of the `start` and `end` parameters,
                // so to read `pieceSize` bytes we need to go to `pieceSize - 1`.
                GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key)
                    .withRange(currentPosition, currentPosition + bufferSize - 1);

                currentPosition += bufferSize;

                return s3Client.getObject(getObjectRequest).getObjectContent();
            }
        };
    }

    private Enumeration<? extends InputStream> getBufferedEnumeration(Enumeration<S3ObjectInputStream> underlying) {
        return new Enumeration<ByteArrayInputStream>() {

            @Override
            public boolean hasMoreElements() {
                return underlying.hasMoreElements();
            }

            @Override
            public ByteArrayInputStream nextElement() {
                S3ObjectInputStream nextStream = underlying.nextElement();
                byte[] byteArray = new byte[0];
                try {
                    byteArray = IOUtils.toByteArray(nextStream);
                    nextStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return new ByteArrayInputStream(byteArray);
            }

        };
    }
}