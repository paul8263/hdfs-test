package com.paultech;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CreateFileTest {
    public static final String BASE_DIR = "/benchmark";

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption("b", "batchSize", true, "Batch Size")
                .addOption("t", "threadCount", true, "Number of threads")
                .addOption("r", "rounds", true, "Number of rounds")
                .addOption("u", "url", true, "HDFS URL")
                .addOption("d", "needsWriteData", false, "Needs write data");
        return options;
    }

    public static void main(String[] args) throws Exception {
        DefaultParser defaultParser = new DefaultParser();
        CommandLine commandLine = defaultParser.parse(buildOptions(), args);
        String hdfsUrl = commandLine.getOptionValue("u", "hdfs://10.180.210.228:8020/");
        int batchSize = Integer.parseInt(commandLine.getOptionValue("b", "1000"));
        int rounds = Integer.parseInt(commandLine.getOptionValue("r", "1"));
        int threadCount = Integer.parseInt(commandLine.getOptionValue("t", "8"));
        boolean needsWriteData = commandLine.hasOption("d");

        System.out.println("HDFS URL: " + hdfsUrl);
        System.out.println("Batch size: " + batchSize);
        System.out.println("Round: " + rounds);
        System.out.println("Thread count: " + threadCount);
        System.out.println("Needs write data: " + needsWriteData);

        FileSystem fileSystem = FileSystem.get(new URI(hdfsUrl), new Configuration(), "hdfs");

        if (fileSystem.exists(new Path(BASE_DIR))) {
            System.out.println("benchmark base dir " + BASE_DIR + " exists. Clean it before running benchmark.");
            fileSystem.delete(new Path(BASE_DIR), true);
            System.out.println("benchmark base dir " + BASE_DIR + " has been cleaned.");
        }
        fileSystem.mkdirs(new Path(BASE_DIR));
        System.out.println("Created benchmark base dir: " + BASE_DIR);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch countDownLatch = new CountDownLatch(rounds);

        for (int i = 0; i < rounds; i++) {
            executor.execute(new CreateFileJob("test" + i, fileSystem, batchSize, needsWriteData, countDownLatch));
        }

        countDownLatch.await();
        fileSystem.close();
        executor.shutdown();

//        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fileSystem.listFiles(new Path("/benchmark/"), true);
//        while (fileStatusRemoteIterator.hasNext()) {
//            System.out.println(fileStatusRemoteIterator.next().getPath());
//        }

    }
}

class CreateFileJob implements Runnable {
    private final String prefix;

    private final FileSystem fileSystem;

    private final CountDownLatch countDownLatch;

    private final int batchSize;

    private final boolean needsWriteData;

    public CreateFileJob(String prefix, FileSystem fileSystem, int batchSize, boolean needsWriteData, CountDownLatch countDownLatch) {
        this.prefix = prefix;
        this.fileSystem = fileSystem;
        this.batchSize = batchSize;
        this.countDownLatch = countDownLatch;
        this.needsWriteData = needsWriteData;
    }

    @Override
    public void run() {
        try {
            fileSystem.mkdirs(new Path(CreateFileTest.BASE_DIR + Path.SEPARATOR + prefix));
            for (int i = 0; i < batchSize; i++) {
                Path filePath = new Path(CreateFileTest.BASE_DIR + Path.SEPARATOR + prefix + Path.SEPARATOR + i);
                fileSystem.createNewFile(filePath);
                if (needsWriteData) {
                    writeData(filePath, "test");
                }
            }
            System.out.println("Finished thread for creating: " + this.prefix);
            countDownLatch.countDown();
            System.out.println("Round remains: " + countDownLatch.getCount());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeData(Path path, String content) {
        try {
            FSDataOutputStream fsDataOutputStream = fileSystem.append(path);
            fsDataOutputStream.writeChars(content);
            fsDataOutputStream.flush();
            fsDataOutputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}