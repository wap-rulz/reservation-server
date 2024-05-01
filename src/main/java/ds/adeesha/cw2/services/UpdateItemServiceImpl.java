package ds.adeesha.cw2.services;

import ds.adeesha.cw2.ReservationServer;
import ds.adeesha.cw2.grpc.UpdateItemServiceGrpc;
import ds.adeesha.synchronization.DistributedTxListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UpdateItemServiceImpl extends UpdateItemServiceGrpc.UpdateItemServiceImplBase implements DistributedTxListener {
    private static final Logger logger = LogManager.getLogger(UpdateItemServiceImpl.class);

    private final ReservationServer server;

    public UpdateItemServiceImpl(ReservationServer server) {
        this.server = server;
    }

    @Override
    public void onGlobalCommit() {

    }

    @Override
    public void onGlobalAbort() {

    }
}
