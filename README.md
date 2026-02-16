[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/plf-smPw)
# CSM218: Parallel and Distributed Computing - Matrix Assignment

## Problem Description

In this assignment, you will implement a **distributed task coordination system** that performs matrix operations. Unlike standard socket programs, you are required to design a custom communication protocol and manage a dynamic pool of worker processes.

### Objectives

1.  **Custom Protocol Design**: Define a wire format for the `Message` class in `Message.java`. Do NOT use JSON or standard Java Serialization.
2.  **Distributed Coordination**: Implement the `Master` class to partition matrix data, schedule tasks, and handle "Stragglers" (slow workers).
3.  **High-Concurrency Workers**: Implement the `Worker` class as a concurrent node that joins the cluster and executes tasks asynchronously.
4.  **Failure Recovery**: Master must detect worker failure via heartbeat timeouts and re-assign tasks to ensure the job completes.

### Project Structure

```text
src/main/java/pdc/
  Master.java    # Distributed Cluster Coordinator
  Worker.java    # Concurrent Execution Node
  Message.java   # Custom Wire Protocol Definition
  MatrixGenerator.java # Utility (Provided)
autograder/
  grade.py       # Local grading script
  tests/         # Python integration tests
```

---

## Local Testing Guide

To verify your solution before pushing to GitHub, follow these steps:

### 1. Prerequisites
- **Java 11** or higher.
- **Python 3.10** or higher.
- **Gradle** (already included via the `./gradlew` wrapper).

### 2. Basic Compilation & Unit Tests
Run the basic checks to ensure your code is syntactically correct:
```bash
# Unix/Mac/Git Bash
./gradlew build
./gradlew test

# Windows (PowerShell)
.\gradlew.bat build
.\gradlew.bat test
```

### 3. Running the Full Autograder
The autograder performs integration tests, checking for parallel execution timing and failure recovery. **Note: A simple implementation that runs sequentially will not pass these tests.**

#### On Linux / Mac / Git Bash (Recommended):
```bash
chmod +x autograder/run_autograder.sh
bash autograder/run_autograder.sh
```

#### On Windows (PowerShell):
```powershell
python autograder/grade.py
```

---

## Grading Criteria

The autograder calculates your score based on:
- **Protocol Compliance (20%)**: Does your `Message` class follow the CSM218 schema?
- **IPC Communication (20%)**: Do Master and Worker communicate via your custom wire format?
- **Parallel Execution (20%)**: Does the system actually speed up when more workers are added?
- **Failure Recovery (20%)**: Does the system survive if a worker process is killed?
- **Concurrency (20%)**: Are you using thread-safe collections and proper synchronization?

**Final Mark**: Your score in the GitHub Classroom dashboard is based on these tests. You need at least **60%** to pass this assignment.

---

## Student Workflow (Step-by-Step)

To successfully complete this assignment, follow these steps:

### 1. Accept & Clone
1.  **Accept the Assignment** via the GitHub Classroom link provided by your instructor.
2.  **Clone your repository** to your local machine:
    ```bash
    git clone <your-repo-url>
    cd <your-repo-folder>
    ```

### 2. Implementation Phase
1.  **Design your Protocol**: Start by implementing the `pack` and `unpack` methods in `src/main/java/pdc/Message.java`. You must decide how to handle message boundaries in a TCP stream.
2.  **Scaffold the Master & Worker**:
    *   Initialize the `ServerSocket` in `Master.java`.
    *   Implement the connection logic in `Worker.java`.
3.  **Implement Logic**: Complete the `coordinate` and `execute` methods to perform parallel matrix operations.

### 3. Local Verification
Before pushing, run the autograder locally to check your progress:
- **Architecture Check**: `bash autograder/run_autograder.sh --type static`
- **Full Test**: `bash autograder/run_autograder.sh`

### 4. Commit & Push
When you are satisfied with your local score:
1.  **Stage your changes**: `git add src/main/java/pdc/*.java`
2.  **Commit**: `git commit -m "feat: implement robust failure handling and custom protocol"`
3.  **Push**: `git push origin main`

### 5. Check Results
1.  Go to your repository on GitHub.
2.  Click on the **Actions** tab to see your autograding run.
3.  Once the run is complete, your grade will be visible in the **GitHub Classroom Dashboard**.

---
