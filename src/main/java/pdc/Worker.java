package pdc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {
    private final ExecutorService rpcThreads = Executors.newFixedThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors()), daemonFactory("worker-rpc-"));
    private final ExecutorService ioThreads = Executors.newCachedThreadPool(daemonFactory("worker-io-"));
    private final ScheduledExecutorService healthThreads = Executors
            .newSingleThreadScheduledExecutor(daemonFactory("worker-health-"));
    private final BlockingQueue<Message> inboundQueue = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean executeLoopStarted = new AtomicBoolean(false);
    private final AtomicBoolean healthMonitorStarted = new AtomicBoolean(false);
    private final AtomicLong taskSequence = new AtomicLong(0L);

    private final String workerId;
    private final String studentId;
    private final int heartbeatIntervalMs;
    private final int heartbeatTimeoutMs;
    private final int reconnectDelayMs;

    private volatile String masterHost;
    private volatile int masterPort;
    private volatile Socket socket;
    private volatile DataInputStream input;
    private volatile DataOutputStream output;
    private volatile long lastMasterHeartbeatMs = System.currentTimeMillis();
    private final Object sendLock = new Object();

    public Worker() {
        String generatedId = "worker-" + UUID.randomUUID().toString().substring(0, 8);
        this.workerId = envOrDefault("WORKER_ID", generatedId);
        this.studentId = envOrDefault("STUDENT_ID", this.workerId);
        this.heartbeatIntervalMs = intEnvOrDefault("WORKER_HEARTBEAT_MS", 2000);
        this.heartbeatTimeoutMs = intEnvOrDefault("WORKER_TIMEOUT_MS", 8000);
        this.reconnectDelayMs = intEnvOrDefault("WORKER_RECONNECT_MS", 3000);
        this.masterHost = envOrDefault("MASTER_HOST", "localhost");
        this.masterPort = intEnvOrDefault("MASTER_PORT", intEnvOrDefault("CSM218_PORT_BASE", 9999));
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        this.masterHost = (masterHost == null || masterHost.isBlank()) ? this.masterHost : masterHost;
        this.masterPort = port > 0 ? port : this.masterPort;
        this.running.set(true);

        try {
            connectSocket();
            sendRegistrationHandshake();
            startNetworkReader();
            startHealthMonitor();
            execute();
        } catch (IOException e) {
            // Join failures are non-fatal; recovery loop will retry.
            markDisconnected();
            startHealthMonitor();
        }
    }

    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {
        running.set(true);
        if (!executeLoopStarted.compareAndSet(false, true)) {
            return;
        }

        ioThreads.submit(() -> {
            while (running.get()) {
                try {
                    Message inbound = inboundQueue.poll(500, TimeUnit.MILLISECONDS);
                    if (inbound == null) {
                        continue;
                    }
                    handleInboundMessage(inbound);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception ignored) {
                    // Continue servicing queue despite malformed requests.
                }
            }
        });
    }

    public void shutdown() {
        running.set(false);
        markDisconnected();
        healthThreads.shutdownNow();
        ioThreads.shutdownNow();
        rpcThreads.shutdownNow();
    }

    private void connectSocket() throws IOException {
        Socket connectedSocket = new Socket();
        connectedSocket.connect(new InetSocketAddress(masterHost, masterPort), 3000);
        connectedSocket.setTcpNoDelay(true);
        connectedSocket.setKeepAlive(true);
        connectedSocket.setSoTimeout(Math.max(1000, heartbeatIntervalMs));

        this.socket = connectedSocket;
        this.input = new DataInputStream(new BufferedInputStream(connectedSocket.getInputStream()));
        this.output = new DataOutputStream(new BufferedOutputStream(connectedSocket.getOutputStream()));
        this.connected.set(true);
        this.lastMasterHeartbeatMs = System.currentTimeMillis();
    }

    private void sendRegistrationHandshake() throws IOException {
        sendMessage(buildMessage("CONNECT", workerId));
        sendMessage(buildMessage("REGISTER_WORKER", workerId));
        String capabilities = "threads=" + Runtime.getRuntime().availableProcessors()
                + ";ops=MATRIX_MULTIPLY,BLOCK_MULTIPLY,BLOCK_TRANSPOSE,SUM";
        sendMessage(buildMessage("REGISTER_CAPABILITIES", capabilities));
    }

    private void startNetworkReader() {
        ioThreads.submit(() -> {
            while (running.get() && connected.get()) {
                try {
                    Message inbound = Message.readFrom(input);
                    if (inbound == null) {
                        break;
                    }
                    inboundQueue.offer(inbound);
                } catch (SocketTimeoutException timeout) {
                    if (System.currentTimeMillis() - lastMasterHeartbeatMs > heartbeatTimeoutMs) {
                        break;
                    }
                } catch (IOException e) {
                    break;
                }
            }
            markDisconnected();
        });
    }

    private void startHealthMonitor() {
        if (healthThreads.isShutdown() || !healthMonitorStarted.compareAndSet(false, true)) {
            return;
        }
        healthThreads.scheduleAtFixedRate(() -> {
            if (!running.get()) {
                return;
            }

            if (connected.get()) {
                long now = System.currentTimeMillis();
                if (now - lastMasterHeartbeatMs > heartbeatTimeoutMs) {
                    markDisconnected();
                    return;
                }

                try {
                    sendMessage(buildMessage("HEARTBEAT", "PING"));
                } catch (IOException e) {
                    markDisconnected();
                }
                return;
            }

            tryRecoverConnection();
        }, heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void tryRecoverConnection() {
        try {
            connectSocket();
            sendRegistrationHandshake();
            startNetworkReader();
            execute();
        } catch (IOException e) {
            sleepQuietly(reconnectDelayMs);
        }
    }

    private void handleInboundMessage(Message inbound) {
        if (inbound == null) {
            return;
        }

        String messageType = normalizeType(inbound);
        if ("WORKER_ACK".equals(messageType) || "HEARTBEAT".equals(messageType)) {
            lastMasterHeartbeatMs = System.currentTimeMillis();
            if ("HEARTBEAT".equals(messageType)) {
                try {
                    sendMessage(buildMessage("HEARTBEAT", "ACK"));
                } catch (IOException ignored) {
                    markDisconnected();
                }
            }
            return;
        }

        if ("RPC_REQUEST".equals(messageType)) {
            rpcThreads.submit(() -> executeRpcRequest(inbound));
        }
    }

    private void executeRpcRequest(Message request) {
        String payload = request.getPayloadAsString();
        String[] parts = payload.split(";", 3);
        String taskId = parts.length > 0 && !parts[0].isBlank()
                ? parts[0]
                : "task-" + taskSequence.incrementAndGet();
        String operation = parts.length > 1 ? parts[1] : "UNKNOWN";
        String taskPayload = parts.length > 2 ? parts[2] : "";
        long startNs = System.nanoTime();

        try {
            String result = handleOperation(operation, taskPayload);
            Message done = buildMessage("TASK_COMPLETE", taskId + ";" + result);
            sendMessage(done);
        } catch (Exception error) {
            Message failed = buildMessage("TASK_ERROR", taskId + ";" + error.getMessage());
            try {
                sendMessage(failed);
            } catch (IOException ignored) {
                markDisconnected();
            }
        } finally {
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
            System.out.println("worker=" + workerId + " task=" + taskId
                    + " op=" + operation + " elapsedMs=" + elapsedMs);
        }
    }

    private String handleOperation(String operation, String taskPayload) {
        String op = operation == null ? "" : operation.toUpperCase(Locale.ROOT);
        switch (op) {
        case "MATRIX_MULTIPLY":
        case "BLOCK_MULTIPLY":
            List<int[][]> pair = parseMatrixPair(taskPayload);
            int[][] left = pair.get(0);
            int[][] right = pair.get(1);
            int[][] product = multiply(left, right);
            return encodeMatrix(product);
        case "BLOCK_TRANSPOSE":
        case "TRANSPOSE":
            return encodeMatrix(transpose(parseMatrix(taskPayload)));
        case "SUM":
            return Integer.toString(sumMatrix(parseMatrix(taskPayload)));
        default:
            return taskPayload;
        }
    }

    private List<int[][]> parseMatrixPair(String payload) {
        String[] split = payload == null ? new String[0] : payload.split("\\|", 2);
        int[][] left = split.length > 0 ? parseMatrix(split[0]) : new int[0][0];
        int[][] right = split.length > 1 ? parseMatrix(split[1]) : left;
        List<int[][]> matrices = new ArrayList<>(2);
        matrices.add(left);
        matrices.add(right);
        return matrices;
    }

    private int[][] parseMatrix(String encoded) {
        if (encoded == null || encoded.isBlank()) {
            return new int[0][0];
        }

        String[] rows = encoded.split("\\\\");
        int[][] matrix = new int[rows.length][];
        for (int i = 0; i < rows.length; i++) {
            if (rows[i].isBlank()) {
                matrix[i] = new int[0];
                continue;
            }

            String[] values = rows[i].split(",");
            matrix[i] = new int[values.length];
            for (int j = 0; j < values.length; j++) {
                matrix[i][j] = Integer.parseInt(values[j].trim());
            }
        }
        return matrix;
    }

    private String encodeMatrix(int[][] matrix) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < matrix.length; i++) {
            if (i > 0) {
                builder.append('\\');
            }
            for (int j = 0; j < matrix[i].length; j++) {
                if (j > 0) {
                    builder.append(',');
                }
                builder.append(matrix[i][j]);
            }
        }
        return builder.toString();
    }

    private int[][] multiply(int[][] left, int[][] right) {
        if (left.length == 0 || right.length == 0) {
            return new int[0][0];
        }
        if (left[0].length != right.length) {
            throw new IllegalArgumentException("Matrix dimensions do not align.");
        }

        int rows = left.length;
        int cols = right[0].length;
        int common = right.length;
        int[][] result = new int[rows][cols];

        for (int i = 0; i < rows; i++) {
            for (int k = 0; k < common; k++) {
                int leftVal = left[i][k];
                for (int j = 0; j < cols; j++) {
                    result[i][j] += leftVal * right[k][j];
                }
            }
        }

        return result;
    }

    private int[][] transpose(int[][] matrix) {
        if (matrix.length == 0) {
            return new int[0][0];
        }
        int rows = matrix.length;
        int cols = matrix[0].length;
        int[][] transposed = new int[cols][rows];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                transposed[j][i] = matrix[i][j];
            }
        }
        return transposed;
    }

    private int sumMatrix(int[][] matrix) {
        int total = 0;
        for (int[] row : matrix) {
            for (int value : row) {
                total += value;
            }
        }
        return total;
    }

    private Message buildMessage(String type, String payload) {
        Message msg = new Message();
        msg.magic = Message.PROTOCOL_MAGIC;
        msg.version = Message.PROTOCOL_VERSION;
        msg.messageType = type;
        msg.type = type;
        msg.studentId = studentId;
        msg.sender = studentId;
        msg.timestamp = System.currentTimeMillis();
        msg.setPayloadFromString(payload == null ? "" : payload);
        return msg;
    }

    private void sendMessage(Message message) throws IOException {
        if (!connected.get() || output == null) {
            throw new IOException("Worker is not connected.");
        }
        synchronized (sendLock) {
            Message.writeTo(output, message);
        }
    }

    private String normalizeType(Message message) {
        if (message.messageType != null && !message.messageType.isEmpty()) {
            return message.messageType;
        }
        return message.type == null ? "" : message.type;
    }

    private void markDisconnected() {
        connected.set(false);
        closeQuietly(input);
        closeQuietly(output);
        closeQuietly(socket);
        input = null;
        output = null;
        socket = null;
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Ignore cleanup failures.
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static String envOrDefault(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }

    private static int intEnvOrDefault(String key, int fallback) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException ex) {
            return fallback;
        }
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicLong counter = new AtomicLong(0L);
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    public static void main(String[] args) {
        Worker worker = new Worker();
        String host = envOrDefault("MASTER_HOST", "localhost");
        int port = intEnvOrDefault("MASTER_PORT", intEnvOrDefault("CSM218_PORT_BASE", 9999));
        worker.joinCluster(host, port);
        worker.execute();

        while (true) {
            sleepQuietly(1000);
        }
    }
}
