package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.AbstractServer;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.runtime.ProcessRequestServer;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Created by affo on 01/12/17.
 */
public abstract class AbstractStateOperatorTransactionCloser
        extends AbstractTwoPCParticipant<StateOperatorTransactionCloseListener> {

    protected AbstractStateOperatorTransactionCloser(SubscriptionMode subscriptionMode) {
        super(subscriptionMode);
    }

    @Override
    public NetUtils.ServerType getServerType() {
        return NetUtils.ServerType.STATE;
    }

    @Override
    public Supplier<AbstractServer> getServerSupplier() {
        return StateServer::new;
    }

    protected abstract void onClose(Address coordinatorAddress, String request,
                                    Consumer<Void> onSinkACK, Consumer<Void> onCoordinatorACK,
                                    Consumer<Throwable> error);

    private class StateServer extends ProcessRequestServer {

        @Override
        protected void parseRequest(String request) {
            // LOG.info(srv.getMyAddress() + " " + request);
            CloseTransactionNotification notification = CloseTransactionNotification.deserialize(request);
            long timestamp = notification.timestamp;

            Iterable<StateOperatorTransactionCloseListener> listeners = getListeners(notification)
                    .filter(l -> l.isInterestedIn(timestamp)).collect(Collectors.toList());


            Address coordinatorAddress = null;
            StringBuilder updatesRepresentation = new StringBuilder();

            for (StateOperatorTransactionCloseListener listener : listeners) {
                coordinatorAddress = listener.getCoordinatorAddressForTransaction((int) timestamp);
                if (notification.vote == Vote.COMMIT) {
                    updatesRepresentation.append(listener.getUpdatesRepresentation((int) timestamp));
                }
            }

            String repr = updatesRepresentation.toString();
            if (repr.isEmpty()) {
                repr = "[]";
            }

            onClose(coordinatorAddress, request + "," + repr,
                    aVoid -> {
                        for (StateOperatorTransactionCloseListener listener : listeners) {
                            listener.onTransactionClosedSuccess(notification);
                        }
                    },
                    aVoid -> {
                        for (StateOperatorTransactionCloseListener listener : listeners) {
                            listener.pushTransactionUpdates(notification.timestamp);
                        }
                    },
                    error -> {
                        for (StateOperatorTransactionCloseListener listener : listeners) {
                            listener.onTransactionClosedError(notification, error);
                        }
                    });
        }
    }
}
