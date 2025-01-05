package niklase.broker;

import java.io.IOException;
import java.io.PrintStream;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerGroup {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerGroup.class);
    private final String name;
    private final MessageProcessingFilter messageProcessingFilter;
    private final ReplicationLinks replicationLinks;
    private List<ClientProxy> clients = new LinkedList<>();
    private final Lock lock = new ReentrantLock();
    private ClientProxy current = null;

    private List<String> messages = new LinkedList<>();

    public ConsumerGroup(final String name, final MessageProcessingFilter messageProcessingFilter,
            final ReplicationLinks replicationLinks) {
        this.name = name;
        this.messageProcessingFilter = messageProcessingFilter;
        this.replicationLinks = replicationLinks;
    }

    public synchronized void addConnectedClient(final ClientProxy clientProxy) {
        this.clients.add(clientProxy);
        if (this.clients.stream().filter(clientProxy1 -> clientProxy1.socketToClient() != null).count() == 1) {
            replicationLinks.announceWillingnessToDistributeMessagesFor(this.name);
        }
    }

    public synchronized void accept(final String envelope) {
        logger.info("Storing [{}], acquiring lock...", envelope);
        lock.lock();
        messages.add(envelope);
        lock.unlock();
        this.flush();
    }

    public void setSocket(final String clientName, final Socket socketWithClient) {
        this.clients.stream()
                .filter(clientProxy -> clientProxy.getName().equals(clientName))
                .forEach(clientProxy -> clientProxy.setSocketToClient(socketWithClient));
        this.flush();
    }

    public void clearSocket(final Socket socket) {
        this.clients.forEach(clientProxy -> clientProxy.clearSocketToClient(socket));
    }

    synchronized void flush() {
        var clientProxy = getNextClientProxyWithSocket();
        if (clientProxy != null) {
            var socketToClient = clientProxy.socketToClient();
            try {
                lock.lock();
                messages.removeIf(envelope -> {
                    if (!this.messageProcessingFilter.shouldBeProcessed(envelope)) {
                        return false;
                    }

                    try {
                        new PrintStream(socketToClient.getOutputStream(), true).println(envelope);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    logger.info("<<< >>> forwarded [{}] to {}", envelope, clientProxy.getName());
                    replicationLinks.acceptMessageDeliveryConfirmation(envelope, name);
                    return true;
                });
                logger.info("Flushing {} finished without Exceptions. {} message(s) are left", clientProxy.getName(),
                        this.messages.size());
            } catch (Exception e) {
                try {
                    logger.info("ConsumerGroup {} is closing socket, because of ", this.name, e);
                    socketToClient.close();
                } catch (NullPointerException ex) {
                    logger.info(
                            "Unable to close socket on failed send operation, because socket has been nullified meanwhile");
                } catch (Exception ex) {
                    logger.info("Unable to close socket on failed send operation");
                } finally {
                    clientProxy.clearSocketToClient(socketToClient);
                }
            } finally {
                lock.unlock();
            }
        } else {
            logger.info("Unable to flush consumergroup {}, because there are no clients with socket", this.name);
        }

    }

    protected ClientProxy getNextClientProxyWithSocket() {
        var connectedClients =
                this.clients.stream().filter(clientProxy -> clientProxy.socketToClient() != null).toList();

        // 0 clients
        if (connectedClients.isEmpty()) {
            return null;
        }

        if (current == null) {
            current = connectedClients.get(0);
        } else {
            // 1 client
            if (connectedClients.size() == 1) {
                current = connectedClients.get(0);
                return current;
            }
            // current is at end of list, we have to jump back to index 0
            if (connectedClients.size() == connectedClients.indexOf(current) + 1) {
                current = connectedClients.get(0);
                return current;
            }

            // 1 subsequent client is available (current index =0), size = 2
            if (connectedClients.size() > connectedClients.indexOf(current) + 1) {
                current = connectedClients.get(connectedClients.indexOf(current) + 1);
                return current;
            }
        }
        return current;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ConsumerGroup that = (ConsumerGroup) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }

    public String getName() {
        return this.name;
    }

    public void removeClientProxy(final String clientName) {
        this.clients.removeIf(clientProxy -> clientProxy.getName().equals(clientName));
    }

    public boolean isEmpty() {
        return this.clients.isEmpty();
    }

    public long getTotalMessageCount() {
        return messages.size();
    }

    public void delete(final String envelopeToDelete) {
        lock.lock();
        this.messages.removeIf(m -> m.equals(envelopeToDelete));
        lock.unlock();
    }

    public List<ClientProxy> getClientProxies() {
        return this.clients;
    }
}
