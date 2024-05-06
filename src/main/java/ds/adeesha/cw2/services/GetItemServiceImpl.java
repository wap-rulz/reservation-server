package ds.adeesha.cw2.services;

import ds.adeesha.cw2.ReservationServer;
import ds.adeesha.cw2.grpc.*;
import io.grpc.stub.StreamObserver;

import java.util.List;

public class GetItemServiceImpl extends GetItemServiceGrpc.GetItemServiceImplBase {
    private final ReservationServer server;

    public GetItemServiceImpl(ReservationServer server) {
        this.server = server;
    }

    @Override
    public void getItems(GetItemRequest request, StreamObserver<GetItemResponse> responseObserver) {
        List<Item> items;
        GetItemResponse response;
        if (request.getIsState()) {
            if (server.isLeader()) {
                System.out.println("Get Status Request received");
                // Send state as primary
                synchronized (ReservationServer.class) {
                    items = server.getItems();
                    System.out.println("Sending state as Primary");
                }
            } else {
                // Get state message is not supposed for secondary
                return;
            }
        } else {
            System.out.println("Get Items Request received");
            items = getItems();
        }
        response = GetItemResponse.newBuilder()
                .addAllItems(items)
                .build();
        System.out.println("Sending response with " + items.size() + " items");
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private List<Item> getItems() {
        System.out.println("Get all items");
        return server.getItems();
    }
}
