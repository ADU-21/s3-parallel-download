package com.adu21.s3download;

import java.io.File;

import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;

/**
 * @author LukeDu
 * @date 2021/8/7
 */
public class S3Downloader {

    public static void main(String[] args) throws InterruptedException {
        File tmpFile = new File("/tmp/test.json");

        long startTime = System.currentTimeMillis();
        // Initialize TransferManager.
        TransferManager tx = new TransferManager();

        // Download the Amazon S3 object to a file.
        Download myDownload = tx.download("duyidong-archive", "data/175M_test_data.json", tmpFile);

        // Blocking call to wait until the download finishes.
        myDownload.waitForCompletion();

        // If transfer manager will not be used anymore, shut it down.
        tx.shutdownNow();
        long endTime = System.currentTimeMillis();
        System.out.println("Run time: " + (endTime - startTime) + "ms");
    }
}
