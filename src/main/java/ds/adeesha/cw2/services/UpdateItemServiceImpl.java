package ds.adeesha.cw2.services;

import ds.adeesha.cw2.ReservationServer;
import ds.adeesha.cw2.grpc.*;
import ds.adeesha.synchronization.DistributedTxListener;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;

public class UpdateItemServiceImpl extends UpdateItemServiceGrpc.UpdateItemServiceImplBase implements DistributedTxListener {
    private final ReservationServer server;
    private UpdateItemRequest tempData;
    private boolean status = false;

    public UpdateItemServiceImpl(ReservationServer server) {
        this.server = server;
    }

    private void updateItem() {
        if (tempData != null) {
            server.updateItem(tempData);
            System.out.println("Update item commited" + tempData.getItem().getId());
            tempData = null;
        }
    }

    @Override
    public void updateItem(UpdateItemRequest request, StreamObserver<UpdateItemResponse> responseObserver) {
        if (server.isLeader()) {
            // Act as primary
            try {
                synchronized (ReservationServer.class) {
                    server.setDistributedTxListener(this);
                    server.validateItemIdExists(request.getItem().getId());
                    System.out.println("Updating item as Primary");
                    startDistributedTx(request);
                    updateSecondaryServers(request);
                    System.out.println("Going to perform transaction");
                    server.performTransaction();
                }
            } catch (Exception e) {
                System.out.println("Error while updating item: " + e.getMessage());
            }
        } else {
            // Act As Secondary
            if (request.getIsSentByPrimary()) {
                synchronized (ReservationServer.class) {
                    server.setDistributedTxListener(this);
                    System.out.println("Updating item on secondary, on Primary's command");
                    startDistributedTx(request);
                    server.voteCommit();
                }
            } else {
                UpdateItemResponse primaryResponse = callPrimary(request);
                status = primaryResponse.getStatus();
            }
        }

        UpdateItemResponse response = UpdateItemResponse.newBuilder()
                .setStatus(status)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void startDistributedTx(UpdateItemRequest request) {
        try {
            server.startTransaction(request.getItem().getId());
            tempData = request;
        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private UpdateItemResponse callPrimary(UpdateItemRequest request) {
        System.out.println("Calling Primary server");
        String[] currentLeaderData = server.getLeaderData();
        String ipAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(request, false, ipAddress, port);
    }

    private void updateSecondaryServers(UpdateItemRequest request) throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers");
        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            String ipAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(request, true, ipAddress, port);
        }
    }

    private UpdateItemResponse callServer(UpdateItemRequest request, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Calling Server " + IPAddress + ":" + port);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(IPAddress, port).usePlaintext().build();
        UpdateItemServiceGrpc.UpdateItemServiceBlockingStub clientStub = UpdateItemServiceGrpc.newBlockingStub(channel);
        UpdateItemRequest secondaryRequest = UpdateItemRequest.newBuilder()
                .setItem(request.getItem())
                .setIsSentByPrimary(isSentByPrimary)
                .build();
        return clientStub.updateItem(secondaryRequest);
    }

    @Override
    public void onGlobalCommit() {
        updateItem();
        status = true;
    }

    @Override
    public void onGlobalAbort() {
        tempData = null;
        System.out.println("Transaction Aborted by the Coordinator");
    }
}
