package pdc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * JUnit 5 tests for the Master class.
 * Tests system coordination and asynchronous listener setup.
 */
class MasterTest {

    private Master master;

    @BeforeEach
    void setUp() {
        master = new Master();
    }

    @Test
    void testCoordinate_Structure() {
        // High level test to ensure the coordinator performs computation.
        int[][] matrix = { { 1, 2 }, { 3, 4 } };
        Object result = master.coordinate("SUM", matrix, 1);
        assertInstanceOf(Integer.class, result, "SUM should return an integer result");
        assertEquals(10, ((Integer) result).intValue(), "SUM result should be stable");
    }

    @Test
    void testListen_NoBlocking() {
        assertDoesNotThrow(() -> {
            master.listen(0); // Port 0 uses any available port
        }, "Server listen logic should handle setup without blocking the main thread incorrectly");
    }

    @Test
    void testReconcile_State() {
        assertDoesNotThrow(() -> {
            master.reconcileState();
        }, "State reconciliation should be a callable system maintenance task");
    }
}
