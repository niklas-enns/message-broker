package niklase.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

class ReplicationLinksTest {

    @Test
    void indexOf() {
        assertEquals(0, ReplicationLinks.indexOf(List.of(3, 2), 1));
        assertEquals(1, ReplicationLinks.indexOf(List.of(3, 1), 2));
        assertEquals(2, ReplicationLinks.indexOf(List.of(2, 1), 3));
    }
}