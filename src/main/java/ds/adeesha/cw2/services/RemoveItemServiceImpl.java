package ds.adeesha.cw2.services;

import ds.adeesha.cw2.ReservationServer;
import ds.adeesha.cw2.grpc.RemoveItemServiceGrpc;
import ds.adeesha.synchronization.DistributedTxListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RemoveItemServiceImpl extends RemoveItemServiceGrpc.RemoveItemServiceImplBase implements DistributedTxListener {
    private static final Logger logger = LogManager.getLogger(RemoveItemServiceImpl.class);

    private final ReservationServer server;

    public RemoveItemServiceImpl(ReservationServer server) {
        this.server = server;
    }

    @Override
    public void onGlobalCommit() {

    }

    @Override
    public void onGlobalAbort() {

    }
}
