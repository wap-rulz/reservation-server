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

public class ReserveItemServiceImpl extends ReserveItemServiceGrpc.ReserveItemServiceImplBase implements DistributedTxListener {
    private final ReservationServer server;
    private ReserveItemRequest tempData;
    private boolean transactionStatus = false;
    private String narration = Constants.FAILED_NARRATION;

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
        synchronized (ReservationServer.class) {
            server.setDistributedTxListener(this);
            String id = request.getId();
            String reservationDate = request.getReservationDate();
            boolean isAlreadyReserved = server.checkItemAlreadyReserved(id, reservationDate);
            if (isAlreadyReserved) {
                transactionStatus = false;
            } else {
                if (server.isLeader()) {
                    // Act as primary
                    try {
                        System.out.println("Reserving item as Primary");
                        startDistributedTx(request);
                        updateSecondaryServers(id);
                        System.out.println("Going to perform transaction");
                        server.performTransaction();
                    } catch (Exception e) {
                        System.out.println("Error while reserving item: " + e.getMessage());
                    }
                } else {
                    // Act As Secondary
                    if (request.getIsSentByPrimary()) {
                        System.out.println("Reserving item on secondary, on Primary's command");
                        startDistributedTx(request);
                        server.voteCommit();
                    } else {
                        callPrimary(id);
                    }
                }
            }
            ReserveItemResponse response = ReserveItemResponse.newBuilder()
                    .setStatus(transactionStatus)
                    .setNarration(narration)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    private ReserveItemResponse callPrimary(String id) {
        System.out.println("Calling Primary server");
        String[] currentLeaderData = server.getLeaderData();
        String ipAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(id, false, ipAddress, port);
    }

    private void updateSecondaryServers(String id) throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers");
        List<String[]> othersData = server.getOthersData();
        ReserveItemResponse reserveItemResponse;
        for (String[] data : othersData) {
            String ipAddress = data[0];
            int port = Integer.parseInt(data[1]);
            reserveItemResponse = callServer(id, true, ipAddress, port);
            if (!reserveItemResponse.getStatus()) {
                transactionStatus = false;
            }
        }
    }

    private ReserveItemResponse callServer(String id, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Calling Server " + IPAddress + ":" + port);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(IPAddress, port).usePlaintext().build();
        ReserveItemServiceGrpc.ReserveItemServiceBlockingStub clientStub = ReserveItemServiceGrpc.newBlockingStub(channel);
        ReserveItemRequest request = ReserveItemRequest.newBuilder().setId(id).setIsSentByPrimary(isSentByPrimary).build();
        return clientStub.reserveItem(request);
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
