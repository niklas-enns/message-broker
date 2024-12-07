package niklase.broker;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Objects;

public class Node {

    public String id;
    public Socket socketOfEstablishedReplicationLink;
    public InetSocketAddress addressForNewReplicationLinks;

    Node(final String id, Socket socketOfEstablishedReplicationLink, InetSocketAddress addressForNewReplicationLinks) {
        this.id = id;
        this.socketOfEstablishedReplicationLink = socketOfEstablishedReplicationLink;
        this.addressForNewReplicationLinks = addressForNewReplicationLinks;
    }

    public InetSocketAddress getAddressForNewReplicationLinks() {
        return addressForNewReplicationLinks;
    }

    public void setAddressForNewReplicationLinks(final InetSocketAddress addressForNewReplicationLinks) {
        this.addressForNewReplicationLinks = addressForNewReplicationLinks;
    }

    public Socket getSocketOfEstablishedReplicationLink() {
        return socketOfEstablishedReplicationLink;
    }

    public void setSocketOfEstablishedReplicationLink(final Socket socketOfEstablishedReplicationLink) {
        this.socketOfEstablishedReplicationLink = socketOfEstablishedReplicationLink;
    }

    public void setNodeId(final String id) {
        this.id = id;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Node node = (Node) o;
        return Objects.equals(id, node.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }
}
