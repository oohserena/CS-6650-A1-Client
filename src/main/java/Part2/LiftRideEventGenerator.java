package Part2;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import Class.LiftRide;

public class LiftRideEventGenerator implements Runnable {
    private final BlockingQueue<LiftRide> liftRideQueue;
    private final int totalEvents;
    private final AtomicInteger generatedCount;

    public LiftRideEventGenerator(BlockingQueue<LiftRide> liftRideQueue, int totalEvents, AtomicInteger generatedCount) {
        this.liftRideQueue = liftRideQueue;
        this.totalEvents = totalEvents;
        this.generatedCount = generatedCount;
    }

    @Override
    public void run() {
        Random random = new Random();
        for (int i = 0; i < totalEvents; i++) {
            LiftRide liftRide = new LiftRide();
            liftRide.setSkierID(random.nextInt(100000) + 1); // 1 to 100000
            liftRide.setResortID(random.nextInt(10) + 1);    // 1 to 10
            liftRide.setLiftID(random.nextInt(40) + 1);      // 1 to 40
            liftRide.setSeasonID("2024");                    // Fixed
            liftRide.setDayID(1);                             // Fixed
            liftRide.setTime(random.nextInt(360) + 1);       // 1 to 360

            try {
                liftRideQueue.put(liftRide);
                generatedCount.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("LiftRideEventGenerator interrupted.");
                break;
            }
        }
        System.out.println("LiftRideEventGenerator completed generating " + totalEvents + " events.");
    }
}

