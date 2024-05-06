package ds.adeesha.cw2.services;

import ds.adeesha.cw2.ReservationServer;
import ds.adeesha.cw2.grpc.GetItemRequest;
import ds.adeesha.cw2.grpc.GetItemResponse;
import ds.adeesha.cw2.grpc.GetItemServiceGrpc;
import ds.adeesha.cw2.grpc.Item;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.List;

public class SyncStateService {
    public static void syncState(ReservationServer reservationServer, String host, int port) {
        System.out.println("Initializing Connecting to primary at " + host + ":" + port);
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();
        GetItemServiceGrpc.GetItemServiceBlockingStub clientStub = GetItemServiceGrpc.newBlockingStub(channel);
        GetItemRequest request = GetItemRequest.newBuilder()
                .setIsState(true)
                .build();
        GetItemResponse response = clientStub.getItems(request);
        List<Item> itemList = response.getItemsList();
        for (Item item : itemList) {
            reservationServer.putItem(item);
        }
        channel.shutdown();
    }
}
