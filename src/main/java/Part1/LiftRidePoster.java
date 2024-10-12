package Part1;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import Class.LiftRide;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class LiftRidePoster implements Runnable {
    private final BlockingQueue<LiftRide> liftRideQueue;
    private final int requestsToSend;
    private final AtomicInteger successCount;
    private final AtomicInteger failureCount;

    public LiftRidePoster(BlockingQueue<LiftRide> liftRideQueue, int requestsToSend,
                          AtomicInteger successCount, AtomicInteger failureCount) {
        this.liftRideQueue = liftRideQueue;
        this.requestsToSend = requestsToSend;
        this.successCount = successCount;
        this.failureCount = failureCount;
    }

    @Override
    public void run() {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        for (int i = 0; i < requestsToSend; i++) {
            LiftRide liftRide = null;
            try {
                liftRide = liftRideQueue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("LiftRidePoster interrupted while taking from queue.");
                break;
            }

            // Paste the real URL here please
            String url = String.format("http://localhost:8080/JavaServlets_war_exploded/skiers/%d/seasons/%s/day/%d/skier/%d",
                    liftRide.getResortID(),
                    liftRide.getSeasonID(),
                    liftRide.getDayID(),
                    liftRide.getSkierID());


            JSONObject jsonPayload = new JSONObject();
            jsonPayload.put("time", liftRide.getTime());
            jsonPayload.put("liftID", liftRide.getLiftID());

            boolean success = false;
            int retries = 5;

            while (retries > 0 && !success) {
                try {
                    HttpPost postRequest = new HttpPost(url);
                    postRequest.setHeader("Content-Type", "application/json");
                    postRequest.setEntity(new StringEntity(jsonPayload.toString()));

                    HttpResponse response = httpClient.execute(postRequest);
                    int statusCode = response.getStatusLine().getStatusCode();
                    EntityUtils.consume(response.getEntity());

                    if (statusCode == 201) {
                        successCount.incrementAndGet();
                        success = true;
                    } else if (statusCode >= 400 && statusCode < 600) {
                        retries--;
                        if (retries == 0) {
                            failureCount.incrementAndGet();
                            System.err.println("Failed to send request after 5 retries. Status Code: " + statusCode);
                        }
                    } else {
                        failureCount.incrementAndGet();
                        System.err.println("Unexpected status code: " + statusCode);
                        success = true;
                    }
                } catch (Exception e) {
                    retries--;
                    if (retries == 0) {
                        failureCount.incrementAndGet();
                        System.err.println("Exception occurred while sending request: " + e.getMessage());
                    }
                }
            }
        }

        try {
            httpClient.close();
        } catch (IOException e) {
            System.err.println("Error closing HttpClient: " + e.getMessage());
        }
    }
}

