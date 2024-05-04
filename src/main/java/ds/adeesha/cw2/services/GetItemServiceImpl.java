package ds.adeesha.cw2.services;

import ds.adeesha.cw2.ReservationServer;
import ds.adeesha.cw2.grpc.GetItemRequest;
import ds.adeesha.cw2.grpc.GetItemResponse;
import ds.adeesha.cw2.grpc.GetItemServiceGrpc;
import ds.adeesha.cw2.grpc.Item;
import io.grpc.stub.StreamObserver;

import java.util.List;

public class GetItemServiceImpl extends GetItemServiceGrpc.GetItemServiceImplBase {
    private final ReservationServer server;

    public GetItemServiceImpl(ReservationServer server) {
        this.server = server;
    }

    @Override
    public void getItems(GetItemRequest request, StreamObserver<GetItemResponse> responseObserver) {
        System.out.println("Request received");
        List<Item> items = getItems();
        GetItemResponse response = GetItemResponse.newBuilder().addAllItems(items).build();
        System.out.println("Sending response with " + items.size() + " items");
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private List<Item> getItems() {
        System.out.println("Get all items");
        return server.getItems();
    }
}
