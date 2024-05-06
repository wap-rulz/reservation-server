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

public class ReserveItemServiceImpl extends ReserveItemServiceGrpc.ReserveItemServiceImplBase implements DistributedTxListener {
    private final ReservationServer server;
    private ReserveItemRequest tempData;
    private boolean status = false;

    public ReserveItemServiceImpl(ReservationServer server) {
        this.server = server;
    }

    private void reserveItem() {
        if (tempData != null) {
            server.reserveItem(tempData);
            System.out.println("Reserve item commited: " + tempData.getId());
            tempData = null;
        }
    }

    @Override
    public void reserveItem(ReserveItemRequest request, StreamObserver<ReserveItemResponse> responseObserver) {
        if (server.isLeader()) {
            // Act as primary
            try {
                synchronized (ReservationServer.class) {
                    server.setDistributedTxListener(this);
                    server.validateItemIdExists(request.getId());
                    server.validateItemAlreadyReserved(request.getId(), request.getReservationDate());
                    System.out.println("Reserving item as Primary");
                    startDistributedTx(request);
                    updateSecondaryServers(request);
                    System.out.println("Going to perform transaction");
                    server.performTransaction();
                }
            } catch (Exception e) {
                System.out.println("Error while reserving item: " + e.getMessage());
            }
        } else {
            // Act As Secondary
            if (request.getIsSentByPrimary()) {
                synchronized (ReservationServer.class) {
                    server.setDistributedTxListener(this);
                    System.out.println("Reserving item on secondary, on Primary's command");
                    startDistributedTx(request);
                    server.voteCommit();
                }
            } else {
                ReserveItemResponse primaryResponse = callPrimary(request);
                status = primaryResponse.getStatus();
            }

        }
        ReserveItemResponse response = ReserveItemResponse.newBuilder()
                .setStatus(status)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private ReserveItemResponse callPrimary(ReserveItemRequest request) {
        System.out.println("Calling Primary server");
        String[] currentLeaderData = server.getLeaderData();
        String ipAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(request, false, ipAddress, port);
    }

    private void updateSecondaryServers(ReserveItemRequest request) throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers");
        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            String ipAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(request, true, ipAddress, port);
        }
    }

    private ReserveItemResponse callServer(ReserveItemRequest request, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Calling Server " + IPAddress + ":" + port);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(IPAddress, port).usePlaintext().build();
        ReserveItemServiceGrpc.ReserveItemServiceBlockingStub clientStub = ReserveItemServiceGrpc.newBlockingStub(channel);
        ReserveItemRequest secondaryRequest = ReserveItemRequest.newBuilder()
                .setId(request.getId())
                .setCustomerNo(request.getCustomerNo())
                .setReservationDate(request.getReservationDate())
                .setIsSentByPrimary(isSentByPrimary)
                .build();
        return clientStub.reserveItem(secondaryRequest);
    }

    private void startDistributedTx(ReserveItemRequest request) {
        try {
            server.startTransaction(request.getId());
            tempData = request;
        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    @Override
    public void onGlobalCommit() {
        reserveItem();
        status = true;
    }

    @Override
    public void onGlobalAbort() {
        tempData = null;
        System.out.println("Transaction Aborted by the Coordinator");
    }
}
