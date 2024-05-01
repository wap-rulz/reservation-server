package ds.adeesha.cw2;

import ds.adeesha.cw2.grpc.Item;
import ds.adeesha.cw2.services.AddItemServiceImpl;
import ds.adeesha.cw2.services.GetItemServiceImpl;
import ds.adeesha.cw2.services.RemoveItemServiceImpl;
import ds.adeesha.cw2.services.UpdateItemServiceImpl;
import ds.adeesha.cw2.utility.Constants;
import ds.adeesha.synchronization.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReservationServer {
    private static final Logger logger = LogManager.getLogger(ReservationServer.class);

    private final int serverPort;
    private final DistributedLock leaderLock;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private final GetItemServiceImpl getItemService;
    private final AddItemServiceImpl addItemService;
    private final UpdateItemServiceImpl updateItemService;
    private final RemoveItemServiceImpl removeItemService;
    private DistributedTx transaction;
    private byte[] leaderData;

    private final HashMap<String, Item> items = new HashMap<>();

    public ReservationServer(String host, int port) throws IOException, InterruptedException, KeeperException {
        this.serverPort = port;
        leaderLock = new DistributedLock("ReservationServerCluster", buildServerData(host, port));
        getItemService = new GetItemServiceImpl(this);
        addItemService = new AddItemServiceImpl(this);
        updateItemService = new UpdateItemServiceImpl(this);
        removeItemService = new RemoveItemServiceImpl(this);
        transaction = new DistributedTxParticipant();
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DistributedLock.setZooKeeperURL("localhost:2181");
        if (args.length != 1) {
            logger.warn("Usage: ReservationServer <port>");
            System.exit(1);
        }
        int serverPort = Integer.parseInt(args[0].trim());
        ReservationServer server = new ReservationServer("localhost", serverPort);
        server.startServer();
    }

    private void startServer() throws IOException, InterruptedException, KeeperException {
        Server server = ServerBuilder
                .forPort(serverPort)
                .addService(getItemService)
                .addService(addItemService)
                .addService(updateItemService)
                .addService(removeItemService)
                .build();
        server.start();
        logger.info("ReservationServer Started and ready to accept requests on port: {}", serverPort);
        tryToBeLeader();
        server.awaitTermination();
    }

    public synchronized void setLeaderData(byte[] leaderData) {
        this.leaderData = leaderData;
    }

    public synchronized String[] getLeaderData() {
        return new String(leaderData).split(Constants.COLON);
    }

    private String buildServerData(String IP, int port) {
        return IP + Constants.COLON + port;
    }

    public boolean isLeader() {
        return isLeader.get();
    }

    private void tryToBeLeader() {
        Thread leaderCampaignThread = new Thread(new LeaderCampaignThread());
        leaderCampaignThread.start();
    }

    private void beTheLeader() {
        logger.info("I got the leader lock. Now acting as primary server");
        isLeader.set(true);
        transaction = new DistributedTxCoordinator();
    }

    public void performTransaction() throws InterruptedException, KeeperException {
        ((DistributedTxCoordinator) transaction).perform();
    }

    public void voteCommit() {
        ((DistributedTxParticipant) transaction).voteCommit();
    }

    public void voteAbort() {
        ((DistributedTxParticipant) transaction).voteAbort();
    }

    public void startTransaction(Item item) throws IOException {
        transaction.start(item.getId(), String.valueOf(UUID.randomUUID()));
    }

    public void setDistributedTxListener(DistributedTxListener listener) {
        this.transaction.setListener(listener);
    }

    private class LeaderCampaignThread implements Runnable {
        private byte[] currentLeaderData = null;

        @Override
        public void run() {
            logger.info("Starting the leader campaign");
            try {
                boolean leader = leaderLock.tryAcquireLock();
                while (!leader) {
                    byte[] leaderData = leaderLock.getLockHolderData();
                    if (currentLeaderData != leaderData) {
                        currentLeaderData = leaderData;
                        setLeaderData(currentLeaderData);
                    }
                    Thread.sleep(10000);
                    leader = leaderLock.tryAcquireLock();
                }
                currentLeaderData = null;
                beTheLeader();
            } catch (Exception e) {
                logger.error("Error: {}", e.getMessage());
            }
        }
    }

    public List<String[]> getOthersData() throws KeeperException, InterruptedException {
        List<String[]> result = new ArrayList<>();
        List<byte[]> othersData = leaderLock.getOthersData();
        for (byte[] data : othersData) {
            String[] dataStrings = new String(data).split(":");
            result.add(dataStrings);
        }
        return result;
    }

    public List<Item> getItems() {
        return new ArrayList<>(items.values());
    }

    public void addItem(Item item) {
        if (items.containsKey(item.getId())) {
            throw new RuntimeException("Item already exists");
        } else {
            items.put(item.getId(), item);
        }
    }
}