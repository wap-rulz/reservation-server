package ds.adeesha.cw2.services;

import ds.adeesha.cw2.ReservationServer;
import ds.adeesha.cw2.grpc.GetItemRequest;
import ds.adeesha.cw2.grpc.GetItemResponse;
import ds.adeesha.cw2.grpc.GetItemServiceGrpc;
import ds.adeesha.cw2.grpc.Item;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class GetItemServiceImpl extends GetItemServiceGrpc.GetItemServiceImplBase {
    private static final Logger logger = LogManager.getLogger(GetItemServiceImpl.class);

    private final ReservationServer server;

    public GetItemServiceImpl(ReservationServer server) {
        this.server = server;
    }

    @Override
    public void getItems(GetItemRequest request, StreamObserver<GetItemResponse> responseObserver) {
        logger.info("Request received");
        List<Item> items = getItems();
        GetItemResponse response = GetItemResponse.newBuilder().addAllItems(items).build();
        logger.info("Sending response with {} items", items.size());
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private List<Item> getItems() {
        logger.info("Get all items");
        return server.getItems();
    }
}
