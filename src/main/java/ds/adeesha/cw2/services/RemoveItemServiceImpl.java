package ds.adeesha.cw2.services;

import ds.adeesha.cw2.ReservationServer;
import ds.adeesha.cw2.grpc.*;
import ds.adeesha.synchronization.DistributedTxListener;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;

public class RemoveItemServiceImpl extends RemoveItemServiceGrpc.RemoveItemServiceImplBase implements DistributedTxListener {
    private static final Logger logger = LogManager.getLogger(RemoveItemServiceImpl.class);

    private final ReservationServer server;
    private String itemIdToRemove;
    private boolean transactionStatus;

    public RemoveItemServiceImpl(ReservationServer server) {
        this.server = server;
        this.transactionStatus = true;
    }

    private void removeItem() {
        if (itemIdToRemove != null) {
            server.removeItem(itemIdToRemove);
            logger.info("Remove Item: {} commited", itemIdToRemove);
            itemIdToRemove = null;
        }
    }

    @Override
    public void removeItem(RemoveItemRequest request, StreamObserver<RemoveItemResponse> responseObserver) {
        server.setDistributedTxListener(this);
        String itemId = request.getId();
        if (server.isLeader()) {
            // Act as primary
            try {
                logger.info("Removing item as Primary");
                startDistributedTx(itemId);
                updateSecondaryServers(itemId);
                logger.info("Going to perform transaction");
                server.performTransaction();
            } catch (Exception e) {
                logger.error("Error while removing item: {}", e.getMessage());
            }
        } else {
            // Act As Secondary
            if (request.getIsSentByPrimary()) {
                logger.info("Removing item on secondary, on Primary's command");
                startDistributedTx(itemId);
                server.voteCommit();
            } else {
                RemoveItemResponse response = callPrimary(itemId);
                if (!response.getStatus()) {
                    transactionStatus = false;
                }
            }
        }
        RemoveItemResponse response = RemoveItemResponse.newBuilder().setStatus(transactionStatus).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void startDistributedTx(String id) {
        try {
            server.startTransaction(id);
            itemIdToRemove = id;
        } catch (IOException e) {
            logger.error("Error: {}", e.getMessage());
        }
    }

    private RemoveItemResponse callPrimary(String id) {
        logger.info("Calling Primary server");
        String[] currentLeaderData = server.getLeaderData();
        String ipAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(id, false, ipAddress, port);
    }

    private void updateSecondaryServers(String id) throws KeeperException, InterruptedException {
        logger.info("Updating secondary servers");
        List<String[]> othersData = server.getOthersData();
        RemoveItemResponse removeItemResponse;
        for (String[] data : othersData) {
            String ipAddress = data[0];
            int port = Integer.parseInt(data[1]);
            removeItemResponse = callServer(id, true, ipAddress, port);
            if (!removeItemResponse.getStatus()) {
                transactionStatus = false;
            }
        }
    }

    private RemoveItemResponse callServer(String id, boolean isSentByPrimary, String IPAddress, int port) {
        logger.info("Calling Server {}:{}", IPAddress, port);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(IPAddress, port).usePlaintext().build();
        RemoveItemServiceGrpc.RemoveItemServiceBlockingStub clientStub = RemoveItemServiceGrpc.newBlockingStub(channel);
        RemoveItemRequest request = RemoveItemRequest.newBuilder().setId(id).setIsSentByPrimary(isSentByPrimary).build();
        return clientStub.removeItem(request);
    }

    @Override
    public void onGlobalCommit() {
        removeItem();
    }

    @Override
    public void onGlobalAbort() {
        itemIdToRemove = null;
        logger.warn("Transaction Aborted by the Coordinator");
    }
}
