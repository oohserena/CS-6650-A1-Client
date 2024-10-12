package Part2;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.Collections;
import Class.LiftRide;

public class MultiThreadedClient {
    private static final int TOTAL_REQUESTS = 200_000;
    private static final int INITIAL_THREADS = 32;
    private static final int REQUESTS_PER_THREAD = 1_000;

    public static void main(String[] args) throws InterruptedException {

        BlockingQueue<LiftRide> liftRideQueue = new LinkedBlockingQueue<>(TOTAL_REQUESTS);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicInteger generatedCount = new AtomicInteger(0);


        ArrayList<Long> latencies = new ArrayList<>();

        LiftRideEventGenerator generator = new LiftRideEventGenerator(liftRideQueue, TOTAL_REQUESTS, generatedCount);

        Thread generatorThread = new Thread(generator);
        generatorThread.start();

        // Start time
        long startTime = System.currentTimeMillis();

        // Launch initial 32 threads, each sending 1,000 requests
        for (int i = 0; i < INITIAL_THREADS; i++) {
            LiftRidePoster poster = new LiftRidePoster(liftRideQueue, REQUESTS_PER_THREAD, successCount, failureCount, latencies);
            Thread posterThread = new Thread(poster);
            posterThread.start();
        }

        // Wait for the generator to finish
        generatorThread.join();
        System.out.println("All LiftRides generated.");


        int remainingRequests = TOTAL_REQUESTS - (INITIAL_THREADS * REQUESTS_PER_THREAD);
        System.out.println("Remaining requests to send: " + remainingRequests);


        int additionalThreads = remainingRequests / REQUESTS_PER_THREAD;
        for (int i = 0; i < additionalThreads; i++) {
            LiftRidePoster poster = new LiftRidePoster(liftRideQueue, REQUESTS_PER_THREAD, successCount, failureCount, latencies);
            Thread posterThread = new Thread(poster);
            posterThread.start();
        }

        int extraRequests = remainingRequests % REQUESTS_PER_THREAD;
        if (extraRequests > 0) {
            LiftRidePoster poster = new LiftRidePoster(liftRideQueue, extraRequests, successCount, failureCount, latencies);
            Thread posterThread = new Thread(poster);
            posterThread.start();
        }


        while (successCount.get() + failureCount.get() < TOTAL_REQUESTS) {
            Thread.sleep(1000);
            System.out.println("Progress: " + (successCount.get() + failureCount.get()) + "/" + TOTAL_REQUESTS +
                    " | Success: " + successCount.get() + " | Failure: " + failureCount.get());
        }


        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;


        double throughput = TOTAL_REQUESTS / (totalTime / 1000.0);


        double mean = calculateMean(latencies);
        double median = calculateMedian(latencies);
        double p99 = calculatePercentile(latencies, 99);
        long min = Collections.min(latencies);
        long max = Collections.max(latencies);

        // Print statistics
        System.out.println("========================================");
        System.out.println("Total requests: " + TOTAL_REQUESTS);
        System.out.println("Successful requests: " + successCount.get());
        System.out.println("Unsuccessful requests: " + failureCount.get());
        System.out.println("Total run time: " + totalTime + " ms");
        System.out.println("Throughput: " + throughput + " requests/second");
        System.out.println("Mean latency: " + mean + " ms");
        System.out.println("Median latency: " + median + " ms");
        System.out.println("99th Percentile latency: " + p99 + " ms");
        System.out.println("Min latency: " + min + " ms");
        System.out.println("Max latency: " + max + " ms");
        System.out.println("========================================");
    }

    private static double calculateMean(ArrayList<Long> latencies) {
        return latencies.stream().mapToLong(Long::longValue).average().orElse(0);
    }

    private static double calculateMedian(ArrayList<Long> latencies) {
        Collections.sort(latencies);
        int size = latencies.size();
        if (size % 2 == 0) {
            return (latencies.get(size / 2 - 1) + latencies.get(size / 2)) / 2.0;
        } else {
            return latencies.get(size / 2);
        }
    }

    private static double calculatePercentile(ArrayList<Long> latencies, int percentile) {
        Collections.sort(latencies);
        int index = (int) Math.ceil((percentile / 100.0) * latencies.size()) - 1;
        return latencies.get(Math.min(index, latencies.size() - 1));
    }
}