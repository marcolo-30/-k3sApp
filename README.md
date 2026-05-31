# Smart City Traffic Processor (k3sApp)

This project implements a traffic data processing application designed for a Smart City environment. The application runs on a Kubernetes (K3s) cluster and is designed to be deployed on resource-constrained nodes, such as a Raspberry Pi.

## Architecture

The project consists of the following main components:

- **`traffic_processor.py`**: The core of the application, written in Python. It is responsible for processing data, applying Quality of Service (QoS) logic, and exposing a health endpoint and a dashboard.
- **`app/dashboard.html`**: A simple web dashboard to visualize the application's status and metrics in real-time.
- **`k8s/` Directory**: Contains all the necessary Kubernetes manifests to deploy and configure the application.
- **Observability**: The application is instrumented with OpenTelemetry to export metrics and traces to a collector, and it also interacts with Prometheus to obtain system metrics.

## Features

- **Traffic Data Processing**: Simulates data processing for a smart city application.
- **Web Dashboard**: Provides an interface to monitor the application.
- **Dynamic QoS Management**: Adjusts its behavior based on system metrics like latency, CPU, and memory to maintain service quality.
- **Kubernetes Deployment**: Optimized for K3s, with manifests to manage deployments on specific nodes (e.g., `r3-node`).
- **High Availability**: Uses `startupProbe`, `livenessProbe`, and `readinessProbe` to ensure the pod is healthy.

## Deployment

To deploy the application on your Kubernetes cluster, follow these steps:

1.  **Create the Namespace**:
    ```bash
    kubectl apply -f k8s/namespace.yaml
    ```

2.  **Apply RBAC Configuration (if necessary)**:
    ```bash
    kubectl apply -f k8s/rbac.yaml
    ```

3.  **Expose the Application with a Service**:
    ```bash
    kubectl apply -f k8s/traffic-processor-svc.yaml
    ```

4.  **Deploy the Application**:
    Choose the deployment manifest according to the node where you want the application to run.
    ```bash
    # For the r3-node
    kubectl apply -f k8s/traffic-processor-r3.yaml

    # For the vm1-node
    kubectl apply -f k8s/traffic-processor-vm1.yaml
    ```

## Accessing the Dashboard

Once deployed, the application is accessible via a `NodePort` type `Service`.

- **Node Port (`nodePort`)**: `30080`
- **URL**: `http://<NODE_IP>:30080/app/dashboard.html`

Replace `<NODE_IP>` with the IP address of the Kubernetes node where the pod is running (e.g., the IP of your Raspberry Pi).

## Configuration

The application's configuration is managed through environment variables defined in the `Deployment` files (`k8s/traffic-processor-r3.yaml`). Some of the most important variables are:

| Variable                 | Description                                                              |
| ------------------------ | ------------------------------------------------------------------------ |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | The endpoint of the OpenTelemetry collector.                             |
| `QOS_LATENCY_NORMAL_S`   | The latency threshold considered normal (in seconds).                    |
| `QOS_CPU_NORMAL`         | The CPU usage threshold considered normal (%).                           |
| `QOS_MEM_NORMAL_PCT`     | The memory usage threshold considered normal (%).                        |
| `PROMETHEUS_URL`         | The URL of the Prometheus instance for querying node metrics.            |
| `K8S_NODE_NAME`          | The name of the Kubernetes node that the application should monitor.     |
| `PORT`                   | The port on which the application's web server runs.                     |
