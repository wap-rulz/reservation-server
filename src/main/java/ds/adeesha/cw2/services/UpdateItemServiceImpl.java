package ds.adeesha.cw2.services;

import ds.adeesha.cw2.ReservationServer;
import ds.adeesha.cw2.grpc.*;
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

public class UpdateItemServiceImpl extends UpdateItemServiceGrpc.UpdateItemServiceImplBase implements DistributedTxListener {
    private static final Logger logger = LogManager.getLogger(UpdateItemServiceImpl.class);

    private final ReservationServer server;
    private Pair<String, Item> itemToUpdate;
    private boolean transactionStatus;

    public UpdateItemServiceImpl(ReservationServer server) {
        this.server = server;
        this.transactionStatus = true;
    }

    private void updateItem() {
        if (itemToUpdate != null) {
            server.updateItem(itemToUpdate.getValue());
            logger.info("Update Item: {} commited", itemToUpdate.getKey());
            itemToUpdate = null;
        }
    }

    @Override
    public void updateItem(UpdateItemRequest request, StreamObserver<UpdateItemResponse> responseObserver) {
        server.setDistributedTxListener(this);
        Item item = request.getItem();
        if (server.isLeader()) {
            // Act as primary
            try {
                logger.info("Updating item as Primary");
                startDistributedTx(item);
                updateSecondaryServers(item);
                logger.info("Going to perform transaction");
                server.performTransaction();
            } catch (Exception e) {
                logger.error("Error while updating item: {}", e.getMessage());
            }
        } else {
            // Act As Secondary
            if (request.getIsSentByPrimary()) {
                logger.info("Updating item on secondary, on Primary's command");
                startDistributedTx(item);
                server.voteCommit();
            } else {
                UpdateItemResponse response = callPrimary(item);
                if (!response.getStatus()) {
                    transactionStatus = false;
                }
            }
        }
        UpdateItemResponse response = UpdateItemResponse.newBuilder().setStatus(transactionStatus).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void startDistributedTx(Item item) {
        try {
            server.startTransaction(item.getId());
            itemToUpdate = new Pair<>(item.getName(), item);
        } catch (IOException e) {
            logger.error("Error: {}", e.getMessage());
        }
    }

    private UpdateItemResponse callPrimary(Item item) {
        logger.info("Calling Primary server");
        String[] currentLeaderData = server.getLeaderData();
        String ipAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(item, false, ipAddress, port);
    }

    private void updateSecondaryServers(Item item) throws KeeperException, InterruptedException {
        logger.info("Updating secondary servers");
        List<String[]> othersData = server.getOthersData();
        UpdateItemResponse updateItemResponse;
        for (String[] data : othersData) {
            String ipAddress = data[0];
            int port = Integer.parseInt(data[1]);
            updateItemResponse = callServer(item, true, ipAddress, port);
            if (!updateItemResponse.getStatus()) {
                transactionStatus = false;
            }
        }
    }

    private UpdateItemResponse callServer(Item item, boolean isSentByPrimary, String IPAddress, int port) {
        logger.info("Calling Server {}:{}", IPAddress, port);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(IPAddress, port).usePlaintext().build();
        UpdateItemServiceGrpc.UpdateItemServiceBlockingStub clientStub = UpdateItemServiceGrpc.newBlockingStub(channel);
        UpdateItemRequest request = UpdateItemRequest.newBuilder().setItem(item).setIsSentByPrimary(isSentByPrimary).build();
        return clientStub.updateItem(request);
    }

    @Override
    public void onGlobalCommit() {
        updateItem();
    }

    @Override
    public void onGlobalAbort() {
        itemToUpdate = null;
        logger.warn("Transaction Aborted by the Coordinator");
    }
}
