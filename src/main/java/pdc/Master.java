package pdc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 *
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers).
 * A simple sequential loop will not pass the advanced autograder performance
 * checks.
 */
public class Master {

    private final ExecutorService systemThreads;
    private final ScheduledExecutorService healthThreads;
    private final BlockingQueue<RpcTask> rpcQueue = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<Socket, ConnectionContext> connections = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, WorkerSession> workers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PendingTask> pendingTasks = new ConcurrentHashMap<>();
    private final AtomicBoolean listening = new AtomicBoolean(false);
    private final AtomicLong generatedIds = new AtomicLong(0L);

    private final String studentId;
    private final int heartbeatIntervalMs;
    private final int heartbeatTimeoutMs;
    private final int taskTimeoutMs;
    private final int maxRetryCount;

    private volatile ServerSocket serverSocket;
    private volatile int listenPort = -1;

    public Master() {
        this.systemThreads = Executors.newCachedThreadPool(daemonFactory("master-sys-"));
        this.healthThreads = Executors.newScheduledThreadPool(2, daemonFactory("master-health-"));
        this.studentId = envOrDefault("STUDENT_ID", "master");
        this.heartbeatIntervalMs = intEnvOrDefault("MASTER_HEARTBEAT_MS", 2000);
        this.heartbeatTimeoutMs = intEnvOrDefault("MASTER_WORKER_TIMEOUT_MS", 8000);
        this.taskTimeoutMs = intEnvOrDefault("MASTER_TASK_TIMEOUT_MS", 6000);
        this.maxRetryCount = intEnvOrDefault("MASTER_TASK_RETRY", 3);
    }

    /**
     * Entry point for a distributed computation.
     *
     * Students must:
     * 1. Partition the problem into independent 'computational units'.
     * 2. Schedule units across a dynamic pool of workers.
     * 3. Handle result aggregation while maintaining thread safety.
     *
     * @param operation A string descriptor of the matrix operation (e.g.
     *                  "BLOCK_MULTIPLY")
     * @param data      The raw matrix data to be processed
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (data == null) {
            return null;
        }
        String op = normalize(operation);
        int parallelism = Math.max(1, workerCount > 0 ? workerCount : Runtime.getRuntime().availableProcessors());
        ExecutorService localPool = Executors.newFixedThreadPool(parallelism, daemonFactory("master-local-"));

        try {
            if ("SUM".equals(op)) {
                return parallelRowSumWithRetry(data, localPool);
            }
            if ("MATRIX_MULTIPLY".equals(op) || "BLOCK_MULTIPLY".equals(op)) {
                return parallelSelfMultiplyWithRetry(data, localPool);
            }
            if ("TRANSPOSE".equals(op) || "BLOCK_TRANSPOSE".equals(op)) {
                return transpose(data);
            }
            return parallelRowSumWithRetry(data, localPool);
        } finally {
            localPool.shutdownNow();
        }
    }

    public void listen(int port) throws IOException {
        if (!listening.compareAndSet(false, true)) {
            return;
        }

        int resolvedPort = resolvePort(port);
        this.serverSocket = new ServerSocket(resolvedPort);
        this.serverSocket.setReuseAddress(true);
        this.listenPort = serverSocket.getLocalPort();

        systemThreads.submit(this::acceptLoop);
        systemThreads.submit(this::dispatchLoop);
        healthThreads.scheduleAtFixedRate(this::reconcileState,
                heartbeatIntervalMs,
                heartbeatIntervalMs,
                TimeUnit.MILLISECONDS);
    }

    
    public void reconcileState() {
        long now = System.currentTimeMillis();

        for (WorkerSession session : workers.values()) {
            if (session == null) {
                continue;
            }

            boolean timedOut = now - session.lastHeartbeatMs > heartbeatTimeoutMs;
            if (!session.alive.get() || timedOut || !session.connection.open.get()) {
                session.alive.set(false);
                reassignTasksForWorker(session.workerId, "heartbeat timeout");
                continue;
            }

            try {
                sendMessage(session.connection, buildMessage("HEARTBEAT", "PING"));
            } catch (IOException heartbeatFailure) {
                session.alive.set(false);
                reassignTasksForWorker(session.workerId, "heartbeat send failure");
            }
        }

        workers.entrySet().removeIf(entry -> {
            WorkerSession value = entry.getValue();
            return value != null && !value.alive.get()
                    && (now - value.lastHeartbeatMs) > Math.max(heartbeatTimeoutMs * 3L, 15000L);
        });
    }

    public int getListenPort() {
        return listenPort;
    }

    public void shutdown() {
        listening.set(false);
        closeQuietly(serverSocket);
        for (ConnectionContext context : connections.values()) {
            closeConnection(context);
        }
        healthThreads.shutdownNow();
        systemThreads.shutdownNow();
    }

    private void acceptLoop() {
        while (listening.get()) {
            try {
                Socket socket = serverSocket.accept();
                socket.setKeepAlive(true);
                socket.setTcpNoDelay(true);
                socket.setSoTimeout(Math.max(1000, heartbeatIntervalMs));

                ConnectionContext context = new ConnectionContext("conn-" + generatedIds.incrementAndGet(), socket);
                connections.put(socket, context);
                systemThreads.submit(() -> readLoop(context));
            } catch (SocketException closed) {
                if (!listening.get()) {
                    return;
                }
            } catch (IOException ignored) {
                // Keep accepting subsequent connections.
            }
        }
    }

    private void readLoop(ConnectionContext context) {
        try {
            while (listening.get() && context.open.get()) {
                try {
                    Message inbound = Message.readFrom(context.input);
                    if (inbound == null) {
                        break;
                    }
                    handleMessage(context, inbound);
                } catch (SocketTimeoutException timeout) {
                    if (context.worker && (System.currentTimeMillis() - context.lastHeartbeatMs) > heartbeatTimeoutMs) {
                        break;
                    }
                }
            }
        } catch (IOException ignored) {
            // Connection dropped.
        } finally {
            closeConnection(context);
        }
    }

    private void handleMessage(ConnectionContext context, Message inbound) {
        String type = normalizeMessageType(inbound);
        context.lastHeartbeatMs = System.currentTimeMillis();

        switch (type) {
        case "CONNECT":
            context.peerId = firstNonBlank(inbound.studentId, inbound.sender, inbound.getPayloadAsString(),
                    context.connectionName);
            safeSend(context, buildMessage("WORKER_ACK", context.peerId + ";connected"));
            break;

        case "REGISTER_WORKER":
            registerWorker(context, inbound);
            break;

        case "REGISTER_CAPABILITIES":
            if (context.peerId != null) {
                WorkerSession session = workers.get(context.peerId);
                if (session != null) {
                    session.capabilities = inbound.getPayloadAsString();
                    session.lastHeartbeatMs = System.currentTimeMillis();
                }
            }
            safeSend(context, buildMessage("WORKER_ACK", context.peerId + ";capabilities"));
            break;

        case "HEARTBEAT":
            safeSend(context, buildMessage("HEARTBEAT", "ACK"));
            break;

        case "RPC_REQUEST":
            enqueueRequest(context, inbound);
            break;

        case "TASK_COMPLETE":
        case "RPC_RESPONSE":
            completeFromWorker(inbound.getPayloadAsString(), false);
            break;

        case "TASK_ERROR":
            completeFromWorker(inbound.getPayloadAsString(), true);
            break;

        default:
            break;
        }
    }

    private void registerWorker(ConnectionContext context, Message inbound) {
        String workerId = firstNonBlank(inbound.getPayloadAsString(), inbound.studentId, inbound.sender,
                "worker-" + generatedIds.incrementAndGet());

        context.worker = true;
        context.peerId = workerId;
        context.lastHeartbeatMs = System.currentTimeMillis();

        WorkerSession session = new WorkerSession(workerId, context);
        workers.put(workerId, session);
        safeSend(context, buildMessage("WORKER_ACK", workerId + ";registered"));
    }

    private void enqueueRequest(ConnectionContext requester, Message inbound) {
        String payload = inbound.getPayloadAsString();
        String[] parts = payload.split(";", 3);

        String taskId = parts.length > 0 && !parts[0].isBlank()
                ? parts[0]
                : "task-" + generatedIds.incrementAndGet();
        String taskType = parts.length > 1 ? parts[1] : "SUM";
        String taskPayload = parts.length > 2 ? parts[2] : "";

        rpcQueue.offer(new RpcTask(taskId, taskType, taskPayload, requester, 0, new HashSet<>()));
    }

    private void dispatchLoop() {
        while (listening.get()) {
            try {
                RpcTask task = rpcQueue.poll(500, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }
                dispatch(task);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void dispatch(RpcTask task) {
        WorkerSession worker = chooseWorker(task);
        if (worker == null) {
            systemThreads.submit(() -> executeLocally(task));
            return;
        }

        String payload = task.taskId + ";" + task.taskType + ";" + task.payload;
        Message request = buildMessage("RPC_REQUEST", payload);
        try {
            sendMessage(worker.connection, request);
            worker.activeTasks.incrementAndGet();
            worker.lastHeartbeatMs = System.currentTimeMillis();
            pendingTasks.put(task.taskId,
                    new PendingTask(task, worker.workerId, System.currentTimeMillis() + taskTimeoutMs));
            healthThreads.schedule(() -> timeoutAndReassign(task.taskId, worker.workerId),
                    taskTimeoutMs,
                    TimeUnit.MILLISECONDS);
        } catch (IOException failure) {
            worker.alive.set(false);
            retryOrFail(task, worker.workerId, "dispatch failure");
        }
    }

    private void executeLocally(RpcTask task) {
        try {
            String result = computeLocal(task.taskType, task.payload);
            sendResponse(task.requester, "TASK_COMPLETE", task.taskId, result);
        } catch (Exception error) {
            sendResponse(task.requester, "TASK_ERROR", task.taskId, error.getMessage());
        }
    }

    private void timeoutAndReassign(String taskId, String workerId) {
        PendingTask pending = pendingTasks.get(taskId);
        if (pending == null || !Objects.equals(workerId, pending.workerId)) {
            return;
        }
        if (System.currentTimeMillis() < pending.deadlineMs) {
            return;
        }
        if (!pendingTasks.remove(taskId, pending)) {
            return;
        }

        WorkerSession session = workers.get(workerId);
        if (session != null) {
            session.activeTasks.updateAndGet(v -> Math.max(0, v - 1));
            session.alive.set(false);
        }
        retryOrFail(pending.task, workerId, "task timeout");
    }

    private void completeFromWorker(String payload, boolean isError) {
        String[] parts = payload.split(";", 2);
        String taskId = parts.length > 0 ? parts[0] : "";
        String body = parts.length > 1 ? parts[1] : "";
        if (taskId.isBlank()) {
            return;
        }

        PendingTask pending = pendingTasks.remove(taskId);
        if (pending == null) {
            return;
        }

        WorkerSession session = workers.get(pending.workerId);
        if (session != null) {
            session.activeTasks.updateAndGet(v -> Math.max(0, v - 1));
            session.lastHeartbeatMs = System.currentTimeMillis();
        }

        if (isError) {
            retryOrFail(pending.task, pending.workerId, body);
            return;
        }
        sendResponse(pending.task.requester, "TASK_COMPLETE", taskId, body);
    }

    private void retryOrFail(RpcTask original, String failedWorkerId, String reason) {
        if (original.retryCount >= maxRetryCount) {
            sendResponse(original.requester, "TASK_ERROR", original.taskId, "retry exhausted: " + reason);
            return;
        }
        rpcQueue.offer(original.nextRetry(failedWorkerId));
    }

    private void reassignTasksForWorker(String workerId, String reason) {
        for (Map.Entry<String, PendingTask> entry : pendingTasks.entrySet()) {
            PendingTask pending = entry.getValue();
            if (!Objects.equals(workerId, pending.workerId)) {
                continue;
            }
            if (pendingTasks.remove(entry.getKey(), pending)) {
                retryOrFail(pending.task, workerId, "reassign: " + reason);
            }
        }
    }

    private WorkerSession chooseWorker(RpcTask task) {
        long now = System.currentTimeMillis();
        return workers.values().stream()
                .filter(session -> session.alive.get())
                .filter(session -> session.connection.open.get())
                .filter(session -> (now - session.lastHeartbeatMs) <= heartbeatTimeoutMs)
                .filter(session -> !task.attemptedWorkers.contains(session.workerId))
                .min(Comparator.comparingInt(session -> session.activeTasks.get()))
                .orElse(null);
    }

    private void sendResponse(ConnectionContext requester, String type, String taskId, String body) {
        if (requester == null || !requester.open.get()) {
            return;
        }
        safeSend(requester, buildMessage(type, taskId + ";" + (body == null ? "" : body)));
    }

    private Message buildMessage(String type, String payload) {
        Message message = new Message();
        message.magic = Message.PROTOCOL_MAGIC;
        message.version = Message.PROTOCOL_VERSION;
        message.messageType = type;
        message.type = type;
        message.studentId = studentId;
        message.sender = studentId;
        message.timestamp = System.currentTimeMillis();
        message.setPayloadFromString(payload == null ? "" : payload);
        return message;
    }

    private void sendMessage(ConnectionContext context, Message message) throws IOException {
        if (context == null || !context.open.get()) {
            throw new IOException("Connection closed");
        }
        synchronized (context.writeLock) {
            Message.writeTo(context.output, message);
        }
    }

    private void safeSend(ConnectionContext context, Message message) {
        try {
            sendMessage(context, message);
        } catch (IOException sendFailure) {
            closeConnection(context);
        }
    }

    private void closeConnection(ConnectionContext context) {
        if (context == null || !context.open.compareAndSet(true, false)) {
            return;
        }

        connections.remove(context.socket);
        closeQuietly(context.input);
        closeQuietly(context.output);
        closeQuietly(context.socket);

        if (context.worker && context.peerId != null) {
            WorkerSession session = workers.get(context.peerId);
            if (session != null) {
                session.alive.set(false);
                reassignTasksForWorker(session.workerId, "disconnect");
            }
        }
    }

    private int parallelRowSumWithRetry(int[][] matrix, ExecutorService localPool) {
        if (matrix.length == 0) {
            return 0;
        }

        CountDownLatch done = new CountDownLatch(matrix.length);
        ConcurrentHashMap<Integer, Integer> partial = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, AtomicBoolean> completed = new ConcurrentHashMap<>();

        for (int row = 0; row < matrix.length; row++) {
            completed.put(row, new AtomicBoolean(false));
            scheduleSumRow(matrix, row, 0, localPool, partial, completed, done);
        }

        awaitCompletion(done, "sum timeout");

        int total = 0;
        for (int row = 0; row < matrix.length; row++) {
            total += partial.getOrDefault(row, 0);
        }
        return total;
    }

    private void scheduleSumRow(int[][] matrix,
            int row,
            int attempt,
            ExecutorService localPool,
            ConcurrentHashMap<Integer, Integer> partial,
            ConcurrentHashMap<Integer, AtomicBoolean> completed,
            CountDownLatch done) {
        CompletableFuture.supplyAsync(() -> {
            int sum = 0;
            for (int value : matrix[row]) {
                sum += value;
            }
            return sum;
        }, localPool).orTimeout(taskTimeoutMs, TimeUnit.MILLISECONDS).whenComplete((value, error) -> {
            AtomicBoolean flag = completed.get(row);
            if (error == null) {
                if (flag.compareAndSet(false, true)) {
                    partial.put(row, value);
                    done.countDown();
                }
                return;
            }
            if (attempt < maxRetryCount) {
                scheduleSumRow(matrix, row, attempt + 1, localPool, partial, completed, done);
                return;
            }
            if (flag.compareAndSet(false, true)) {
                partial.putIfAbsent(row, 0);
                done.countDown();
            }
        });
    }

    private int[][] parallelSelfMultiplyWithRetry(int[][] matrix, ExecutorService localPool) {
        validateRectangular(matrix);
        if (matrix.length == 0) {
            return new int[0][0];
        }
        if (matrix[0].length != matrix.length) {
            throw new IllegalArgumentException("Self multiply expects square matrix");
        }

        int size = matrix.length;
        CountDownLatch done = new CountDownLatch(size);
        ConcurrentHashMap<Integer, int[]> partial = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, AtomicBoolean> completed = new ConcurrentHashMap<>();

        for (int row = 0; row < size; row++) {
            completed.put(row, new AtomicBoolean(false));
            scheduleMultiplyRow(matrix, row, 0, localPool, partial, completed, done);
        }

        awaitCompletion(done, "multiply timeout");

        int[][] result = new int[size][size];
        for (int row = 0; row < size; row++) {
            result[row] = partial.getOrDefault(row, new int[size]);
        }
        return result;
    }

    private void scheduleMultiplyRow(int[][] matrix,
            int row,
            int attempt,
            ExecutorService localPool,
            ConcurrentHashMap<Integer, int[]> partial,
            ConcurrentHashMap<Integer, AtomicBoolean> completed,
            CountDownLatch done) {
        CompletableFuture.supplyAsync(() -> {
            int size = matrix.length;
            int[] out = new int[size];
            for (int k = 0; k < size; k++) {
                int left = matrix[row][k];
                for (int j = 0; j < size; j++) {
                    out[j] += left * matrix[k][j];
                }
            }
            return out;
        }, localPool).orTimeout(taskTimeoutMs, TimeUnit.MILLISECONDS).whenComplete((value, error) -> {
            AtomicBoolean flag = completed.get(row);
            if (error == null) {
                if (flag.compareAndSet(false, true)) {
                    partial.put(row, value);
                    done.countDown();
                }
                return;
            }
            if (attempt < maxRetryCount) {
                scheduleMultiplyRow(matrix, row, attempt + 1, localPool, partial, completed, done);
                return;
            }
            if (flag.compareAndSet(false, true)) {
                partial.putIfAbsent(row, new int[matrix.length]);
                done.countDown();
            }
        });
    }

    private void awaitCompletion(CountDownLatch latch, String message) {
        long timeoutMs = Math.max(4000L, taskTimeoutMs * (long) (maxRetryCount + 2));
        try {
            if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException(message);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(message, interrupted);
        }
    }

    private String computeLocal(String taskType, String payload) {
        String op = normalize(taskType);
        switch (op) {
        case "SUM":
            return Integer.toString(sum(parseMatrix(payload)));
        case "TRANSPOSE":
        case "BLOCK_TRANSPOSE":
            return encodeMatrix(transpose(parseMatrix(payload)));
        case "MATRIX_MULTIPLY":
        case "BLOCK_MULTIPLY":
            List<int[][]> pair = parsePair(payload);
            return encodeMatrix(multiply(pair.get(0), pair.get(1)));
        default:
            return payload == null ? "" : payload;
        }
    }

    private List<int[][]> parsePair(String payload) {
        String[] split = payload == null ? new String[0] : payload.split("\\|", 2);
        int[][] left = split.length > 0 ? parseMatrix(split[0]) : new int[0][0];
        int[][] right = split.length > 1 ? parseMatrix(split[1]) : left;
        List<int[][]> pair = new ArrayList<>(2);
        pair.add(left);
        pair.add(right);
        return pair;
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
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < matrix.length; i++) {
            if (i > 0) {
                out.append('\\');
            }
            for (int j = 0; j < matrix[i].length; j++) {
                if (j > 0) {
                    out.append(',');
                }
                out.append(matrix[i][j]);
            }
        }
        return out.toString();
    }

    private int[][] multiply(int[][] left, int[][] right) {
        validateRectangular(left);
        validateRectangular(right);
        if (left.length == 0 || right.length == 0) {
            return new int[0][0];
        }
        if (left[0].length != right.length) {
            throw new IllegalArgumentException("Matrix dimensions do not align");
        }

        int rows = left.length;
        int cols = right[0].length;
        int common = right.length;
        int[][] result = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int k = 0; k < common; k++) {
                int leftValue = left[i][k];
                for (int j = 0; j < cols; j++) {
                    result[i][j] += leftValue * right[k][j];
                }
            }
        }
        return result;
    }

    private int[][] transpose(int[][] matrix) {
        validateRectangular(matrix);
        if (matrix.length == 0) {
            return new int[0][0];
        }
        int rows = matrix.length;
        int cols = matrix[0].length;
        int[][] out = new int[cols][rows];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                out[j][i] = matrix[i][j];
            }
        }
        return out;
    }

    private int sum(int[][] matrix) {
        int total = 0;
        for (int[] row : matrix) {
            for (int value : row) {
                total += value;
            }
        }
        return total;
    }

    private void validateRectangular(int[][] matrix) {
        if (matrix == null || matrix.length == 0) {
            return;
        }
        int cols = matrix[0].length;
        for (int i = 1; i < matrix.length; i++) {
            if (matrix[i].length != cols) {
                throw new IllegalArgumentException("Non-rectangular matrix");
            }
        }
    }

    private int resolvePort(int port) {
        if (port > 0) {
            return port;
        }
        int envPort = intEnvOrDefault("MASTER_PORT", -1);
        if (envPort > 0) {
            return envPort;
        }
        return Math.max(1, intEnvOrDefault("CSM218_PORT_BASE", 9999));
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }

    private static String normalizeMessageType(Message message) {
        if (message.messageType != null && !message.messageType.isEmpty()) {
            return message.messageType;
        }
        return message.type == null ? "" : message.type;
    }

    private static String firstNonBlank(String... options) {
        for (String option : options) {
            if (option != null && !option.isBlank()) {
                return option;
            }
        }
        return "";
    }

    private static String envOrDefault(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
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

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicLong counter = new AtomicLong(0L);
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    private static final class ConnectionContext {
        private final String connectionName;
        private final Socket socket;
        private final DataInputStream input;
        private final DataOutputStream output;
        private final AtomicBoolean open = new AtomicBoolean(true);
        private final Object writeLock = new Object();
        private volatile String peerId;
        private volatile boolean worker;
        private volatile long lastHeartbeatMs = System.currentTimeMillis();

        private ConnectionContext(String connectionName, Socket socket) throws IOException {
            this.connectionName = connectionName;
            this.socket = socket;
            this.input = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            this.output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        }
    }

    private static final class WorkerSession {
        private final String workerId;
        private final ConnectionContext connection;
        private final AtomicBoolean alive = new AtomicBoolean(true);
        private final AtomicInteger activeTasks = new AtomicInteger(0);
        private volatile long lastHeartbeatMs = System.currentTimeMillis();
        private volatile String capabilities = "";

        private WorkerSession(String workerId, ConnectionContext connection) {
            this.workerId = workerId;
            this.connection = connection;
            this.lastHeartbeatMs = System.currentTimeMillis();
        }
    }

    private static final class RpcTask {
        private final String taskId;
        private final String taskType;
        private final String payload;
        private final ConnectionContext requester;
        private final int retryCount;
        private final Set<String> attemptedWorkers;

        private RpcTask(String taskId,
                String taskType,
                String payload,
                ConnectionContext requester,
                int retryCount,
                Set<String> attemptedWorkers) {
            this.taskId = taskId;
            this.taskType = taskType;
            this.payload = payload;
            this.requester = requester;
            this.retryCount = retryCount;
            this.attemptedWorkers = attemptedWorkers;
        }

        private RpcTask nextRetry(String failedWorkerId) {
            Set<String> updated = new HashSet<>(attemptedWorkers);
            if (failedWorkerId != null && !failedWorkerId.isBlank()) {
                updated.add(failedWorkerId);
            }
            return new RpcTask(taskId, taskType, payload, requester, retryCount + 1, updated);
        }
    }

    private static final class PendingTask {
        private final RpcTask task;
        private final String workerId;
        private final long deadlineMs;

        private PendingTask(RpcTask task, String workerId, long deadlineMs) {
            this.task = task;
            this.workerId = workerId;
            this.deadlineMs = deadlineMs;
        }
    }

    public static void main(String[] args) throws Exception {
        Master master = new Master();
        int port = intEnvOrDefault("MASTER_PORT", intEnvOrDefault("CSM218_PORT_BASE", 9999));
        master.listen(port);
        while (true) {
            Thread.sleep(1000);
        }
    }
}
