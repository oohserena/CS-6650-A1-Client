package Part1;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
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


        LiftRideEventGenerator generator = new LiftRideEventGenerator(liftRideQueue, TOTAL_REQUESTS, generatedCount);
        Thread generatorThread = new Thread(generator);
        generatorThread.start();


        long startTime = System.currentTimeMillis();


        for (int i = 0; i < INITIAL_THREADS; i++) {
            LiftRidePoster poster = new LiftRidePoster(liftRideQueue, REQUESTS_PER_THREAD, successCount, failureCount);
            Thread posterThread = new Thread(poster);
            posterThread.start();
        }


        generatorThread.join();
        System.out.println("All LiftRides generated.");


        int remainingRequests = TOTAL_REQUESTS - (INITIAL_THREADS * REQUESTS_PER_THREAD);
        System.out.println("Remaining requests to send: " + remainingRequests);


        int additionalThreads = remainingRequests / REQUESTS_PER_THREAD;
        for (int i = 0; i < additionalThreads; i++) {
            LiftRidePoster poster = new LiftRidePoster(liftRideQueue, REQUESTS_PER_THREAD, successCount, failureCount);
            Thread posterThread = new Thread(poster);
            posterThread.start();
        }


        int extraRequests = remainingRequests % REQUESTS_PER_THREAD;
        if (extraRequests > 0) {
            LiftRidePoster poster = new LiftRidePoster(liftRideQueue, extraRequests, successCount, failureCount);
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

        // Print statistics
        System.out.println("========================================");
        System.out.println("Total requests: " + TOTAL_REQUESTS);
        System.out.println("Successful requests: " + successCount.get());
        System.out.println("Unsuccessful requests: " + failureCount.get());
        System.out.println("Total run time: " + totalTime + " ms");
        System.out.println(String.format("Throughput: %.2f requests/second", throughput));
        System.out.println("========================================");
    }
}

