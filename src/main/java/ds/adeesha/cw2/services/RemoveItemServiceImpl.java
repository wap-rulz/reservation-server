package ds.adeesha.cw2.services;

import ds.adeesha.cw2.ReservationServer;
import ds.adeesha.cw2.grpc.*;
import ds.adeesha.cw2.utility.Constants;
import ds.adeesha.synchronization.DistributedTxListener;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;

public class RemoveItemServiceImpl extends RemoveItemServiceGrpc.RemoveItemServiceImplBase implements DistributedTxListener {
    private final ReservationServer server;
    private RemoveItemRequest tempData;
    private boolean transactionStatus = false;
    private String narration;

    public RemoveItemServiceImpl(ReservationServer server) {
        this.server = server;
    }

    private void removeItem() {
        if (tempData != null) {
            server.removeItem(tempData);
            System.out.println("Remove Item commited: " + tempData.getId());
            tempData = null;
        }
    }

    @Override
    public void removeItem(RemoveItemRequest request, StreamObserver<RemoveItemResponse> responseObserver) {
        synchronized (ReservationServer.class) {
            server.setDistributedTxListener(this);
            if (server.isLeader()) {
                // Act as primary
                try {
                    System.out.println("Removing item as Primary");
                    startDistributedTx(request);
                    updateSecondaryServers(request);
                    System.out.println("Going to perform transaction");
                    server.performTransaction();
                } catch (Exception e) {
                    System.out.println("Error while removing item: " + e.getMessage());
                }
            } else {
                // Act As Secondary
                if (request.getIsSentByPrimary()) {
                    System.out.println("Removing item on secondary, on Primary's command");
                    startDistributedTx(request);
                    server.voteCommit();
                } else {
                    callPrimary(request);
                }
            }
            RemoveItemResponse response = RemoveItemResponse
                    .newBuilder().
                    setStatus(transactionStatus)
                    .setNarration(narration)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    private void startDistributedTx(RemoveItemRequest request) {
        try {
            server.startTransaction(request.getId());
            tempData = request;
        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private RemoveItemResponse callPrimary(RemoveItemRequest request) {
        System.out.println("Calling Primary server");
        String[] currentLeaderData = server.getLeaderData();
        String ipAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(request, false, ipAddress, port);
    }

    private void updateSecondaryServers(RemoveItemRequest request) throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers");
        List<String[]> othersData = server.getOthersData();
        RemoveItemResponse removeItemResponse;
        for (String[] data : othersData) {
            String ipAddress = data[0];
            int port = Integer.parseInt(data[1]);
            removeItemResponse = callServer(request, true, ipAddress, port);
            if (!removeItemResponse.getStatus()) {
                transactionStatus = false;
            }
        }
    }

    private RemoveItemResponse callServer(RemoveItemRequest request, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Calling Server " + IPAddress + ":" + port);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(IPAddress, port).usePlaintext().build();
        RemoveItemServiceGrpc.RemoveItemServiceBlockingStub clientStub = RemoveItemServiceGrpc.newBlockingStub(channel);
        RemoveItemRequest secondaryRequest = RemoveItemRequest.newBuilder().setId(request.getId()).setIsSentByPrimary(isSentByPrimary).build();
        return clientStub.removeItem(secondaryRequest);
    }

    @Override
    public void onGlobalCommit() {
        removeItem();
        narration = Constants.SUCCESS_NARRATION;
    }

    @Override
    public void onGlobalAbort() {
        tempData = null;
        System.out.println("Transaction Aborted by the Coordinator");
        narration = Constants.ABORT_NARRATION;
    }
}
