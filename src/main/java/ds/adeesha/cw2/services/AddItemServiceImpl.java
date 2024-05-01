package ds.adeesha.cw2.services;

import ds.adeesha.cw2.ReservationServer;
import ds.adeesha.cw2.grpc.AddItemRequest;
import ds.adeesha.cw2.grpc.AddItemResponse;
import ds.adeesha.cw2.grpc.AddItemServiceGrpc;
import ds.adeesha.cw2.grpc.Item;
import ds.adeesha.synchronization.DistributedTxListener;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import javafx.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;

public class AddItemServiceImpl extends AddItemServiceGrpc.AddItemServiceImplBase implements DistributedTxListener {
    private static final Logger logger = LogManager.getLogger(AddItemServiceImpl.class);

    private final ReservationServer server;
    private Pair<String, Item> tempData;
    private boolean transactionStatus;

    public AddItemServiceImpl(ReservationServer server) {
        this.server = server;
        this.transactionStatus = true;
    }

    private void addItem() {
        if (tempData != null) {
            server.addItem(tempData.getValue());
            logger.info("Add Item: {} commited", tempData.getKey());
            tempData = null;
        }
    }

    @Override
    public void addItem(AddItemRequest request, StreamObserver<AddItemResponse> responseObserver) {
        synchronized (server) {
            server.setDistributedTxListener(this);
            Item item = request.getItem();
            if (server.isLeader()) {
                // Act as primary
                try {
                    logger.info("Adding new item as Primary");
                    startDistributedTx(item);
                    updateSecondaryServers(item);
                    logger.info("Going to perform transaction");
                    server.performTransaction();
                } catch (Exception e) {
                    logger.error("Error while adding new item: {}", e.getMessage());
                }
            } else {
                // Act As Secondary
                if (request.getIsSentByPrimary()) {
                    logger.info("Adding new item on secondary, on Primary's command");
                    startDistributedTx(item);
                    server.voteCommit();
                } else {
                    AddItemResponse response = callPrimary(item);
                    if (!response.getStatus()) {
                        transactionStatus = false;
                    }
                }
            }
            AddItemResponse response = AddItemResponse.newBuilder().setStatus(transactionStatus).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    private AddItemResponse callPrimary(Item item) {
        logger.info("Calling Primary server");
        String[] currentLeaderData = server.getLeaderData();
        String ipAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(item, false, ipAddress, port);
    }

    private void updateSecondaryServers(Item item) throws KeeperException, InterruptedException {
        logger.info("Updating secondary servers");
        List<String[]> othersData = server.getOthersData();
        AddItemResponse addItemResponse;
        for (String[] data : othersData) {
            String ipAddress = data[0];
            int port = Integer.parseInt(data[1]);
            addItemResponse = callServer(item, true, ipAddress, port);
            if (!addItemResponse.getStatus()) {
                transactionStatus = false;
            }
        }
    }

    private AddItemResponse callServer(Item item, boolean isSentByPrimary, String IPAddress, int port) {
        logger.info("Calling Server {}:{}", IPAddress, port);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(IPAddress, port).usePlaintext().build();
        AddItemServiceGrpc.AddItemServiceBlockingStub clientStub = AddItemServiceGrpc.newBlockingStub(channel);
        AddItemRequest request = AddItemRequest.newBuilder().setItem(item).setIsSentByPrimary(isSentByPrimary).build();
        return clientStub.addItem(request);
    }

    private void startDistributedTx(Item item) {
        try {
            server.startTransaction(item);
            tempData = new Pair<>(item.getId(), item);
        } catch (IOException e) {
            logger.error("Error: {}", e.getMessage());
        }
    }

    @Override
    public void onGlobalCommit() {
        addItem();
    }

    @Override
    public void onGlobalAbort() {
        tempData = null;
        logger.warn("Transaction Aborted by the Coordinator");
    }
}
