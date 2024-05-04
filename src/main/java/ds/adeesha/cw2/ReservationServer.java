package ds.adeesha.cw2;

import ds.adeesha.cw2.grpc.*;
import ds.adeesha.cw2.services.*;
import ds.adeesha.cw2.utility.Constants;
import ds.adeesha.synchronization.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReservationServer {
    private final int serverPort;
    private final DistributedLock leaderLock;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private final GetItemServiceImpl getItemService;
    private final AddItemServiceImpl addItemService;
    private final UpdateItemServiceImpl updateItemService;
    private final RemoveItemServiceImpl removeItemService;
    private final ReserveItemServiceImpl reserveItemService;
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
        reserveItemService = new ReserveItemServiceImpl(this);
        transaction = new DistributedTxParticipant();
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DistributedLock.setZooKeeperURL("localhost:2181");
        if (args.length != 1) {
            System.out.println("Usage: ReservationServer <port>");
            System.exit(1);
        }
        int serverPort = Integer.parseInt(args[0].trim());
        ReservationServer server = new ReservationServer(Constants.LOCALHOST, serverPort);
        server.startServer();
    }

    private void startServer() throws IOException, InterruptedException {
        Server server = ServerBuilder
                .forPort(serverPort)
                .addService(getItemService)
                .addService(addItemService)
                .addService(updateItemService)
                .addService(removeItemService)
                .addService(reserveItemService)
                .build();
        server.start();
        System.out.println("ReservationServer Started and ready to accept requests on port: }" + serverPort);
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
        System.out.println("I got the leader lock. Now acting as primary server");
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

    public void startTransaction(String id) throws IOException {
        transaction.start(id, String.valueOf(UUID.randomUUID()));
    }

    public void setDistributedTxListener(DistributedTxListener listener) {
        this.transaction.setListener(listener);
    }

    private class LeaderCampaignThread implements Runnable {
        private byte[] currentLeaderData = null;

        @Override
        public void run() {
            System.out.println("Starting the leader campaign");
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
                System.out.println("Error: " + e.getMessage());
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

    public void addItem(AddItemRequest request) {
        if (items.containsKey(request.getItem().getId())) {
            throw new RuntimeException("Item already exists");
        } else {
            items.put(request.getItem().getId(), request.getItem());
        }
    }

    public void removeItem(RemoveItemRequest request) {
        if (!items.containsKey(request.getId())) {
            throw new RuntimeException("Item with provided id does not exist");
        } else {
            items.remove(request.getId());
        }
    }

    public void updateItem(UpdateItemRequest request) {
        if (!items.containsKey(request.getItem().getId())) {
            throw new RuntimeException("Item with provided id does not exist");
        } else {
            items.replace(request.getItem().getId(), request.getItem());
        }
    }

    public boolean checkItemAlreadyReserved(String id, String reservationDate) {
        Item item = items.get(id);
        return item.getReservationsMap().containsValue(reservationDate);
    }

    public void reserveItem(ReserveItemRequest request) {
        if (!items.containsKey(request.getId())) {
            throw new RuntimeException("Item with provided id does not exist");
        } else {
            if (!checkItemAlreadyReserved(request.getId(), request.getReservationDate())) {
                Item itemToReserve = items.get(request.getId());
                itemToReserve.getReservationsMap().put(request.getCustomerNo(), request.getReservationDate());
                items.replace(request.getId(), itemToReserve);
            }
        }
    }
}