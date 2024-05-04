package ds.adeesha.cw2.services;

import ds.adeesha.cw2.ReservationServer;
import ds.adeesha.cw2.grpc.AddItemRequest;
import ds.adeesha.cw2.grpc.AddItemResponse;
import ds.adeesha.cw2.grpc.AddItemServiceGrpc;
import ds.adeesha.cw2.utility.Constants;
import ds.adeesha.synchronization.DistributedTxListener;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;

public class AddItemServiceImpl extends AddItemServiceGrpc.AddItemServiceImplBase implements DistributedTxListener {
    private final ReservationServer server;
    private AddItemRequest tempData;
    private boolean transactionStatus = false;
    private String narration;

    public AddItemServiceImpl(ReservationServer server) {
        this.server = server;
    }

    private void addItem() {
        if (tempData != null) {
            server.addItem(tempData);
            System.out.println("Add Item commited: " + tempData.getItem().getId());
            tempData = null;
        }
    }

    @Override
    public void addItem(AddItemRequest request, StreamObserver<AddItemResponse> responseObserver) {
        synchronized (ReservationServer.class) {
            server.setDistributedTxListener(this);
            if (server.isLeader()) {
                // Act as primary
                try {
                    System.out.println("Adding new item as Primary");
                    startDistributedTx(request);
                    updateSecondaryServers(request);
                    System.out.println("Going to perform transaction");
                    server.performTransaction();
                } catch (Exception e) {
                    System.out.println("Error while adding new item: " + e.getMessage());
                }
            } else {
                // Act As Secondary
                if (request.getIsSentByPrimary()) {
                    System.out.println("Adding new item on secondary, on Primary's command");
                    startDistributedTx(request);
                    server.voteCommit();
                } else {
                    callPrimary(request);
                }
            }
            AddItemResponse response = AddItemResponse
                    .newBuilder()
                    .setStatus(transactionStatus)
                    .setNarration(narration)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    private AddItemResponse callPrimary(AddItemRequest request) {
        System.out.println("Calling Primary server");
        String[] currentLeaderData = server.getLeaderData();
        String ipAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(request, false, ipAddress, port);
    }

    private void updateSecondaryServers(AddItemRequest request) throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers");
        List<String[]> othersData = server.getOthersData();
        AddItemResponse addItemResponse;
        for (String[] data : othersData) {
            String ipAddress = data[0];
            int port = Integer.parseInt(data[1]);
            addItemResponse = callServer(request, true, ipAddress, port);
            if (!addItemResponse.getStatus()) {
                transactionStatus = false;
            }
        }
    }

    private AddItemResponse callServer(AddItemRequest request, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Calling Server " + IPAddress + ":" + port);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(IPAddress, port).usePlaintext().build();
        AddItemServiceGrpc.AddItemServiceBlockingStub clientStub = AddItemServiceGrpc.newBlockingStub(channel);
        AddItemRequest secondaryRequest = AddItemRequest.newBuilder().setItem(request.getItem()).setIsSentByPrimary(isSentByPrimary).build();
        return clientStub.addItem(secondaryRequest);
    }

    private void startDistributedTx(AddItemRequest request) {
        try {
            server.startTransaction(request.getItem().getId());
            tempData = request;
        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    @Override
    public void onGlobalCommit() {
        addItem();
        transactionStatus = true;
        narration = Constants.SUCCESS_NARRATION;
    }

    @Override
    public void onGlobalAbort() {
        tempData = null;
        System.out.println("Transaction Aborted by the Coordinator");
        narration = Constants.ABORT_NARRATION;
    }
}
