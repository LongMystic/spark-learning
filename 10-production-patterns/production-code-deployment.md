# Production Pattern: Code Deployment Strategies

When moving Spark applications from local development to production Kubernetes clusters, the way you deliver your Python code (or JARs) changes dramatically. Development environments prioritize fast iteration (e.g., syncing code via `kubectl cp` into a PVC or using ConfigMaps), while production environments prioritize **immutability, version control, and auditability**.

Here are the two dominant strategies for deploying Spark code to production.

---

## 1. The "Baked-In" Strategy (Docker Images)

In this approach, your CI/CD pipeline packages the application code directly into the Spark Docker image.

### How it works
1. **Commit**: You push changes to your Python scripts to Git.
2. **Build**: A CI pipeline (GitHub Actions, Jenkins, GitLab CI) triggers `docker build`. It copies your `requirements.txt` and all your `.py` files into a new image.
3. **Tag**: The image is tagged with the Git commit hash or a release version (e.g., `my-registry.com/spark-etl:v1.2.3`).
4. **Deploy**: Your orchestrator (Airflow/Argo) submits a `SparkApplication` configured to pull `my-registry.com/spark-etl:v1.2.3`.

### Example Dockerfile
```dockerfile
FROM apache/spark:3.5.1
USER root
# Install dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt
# Bake in the code
COPY common/ /opt/spark-apps/common/
COPY jobs/ /opt/spark-apps/jobs/
USER spark
```

### Pros
* **Ultimate Immutability**: The code and the environment (dependencies, Spark version, OS libraries) are locked together forever. Rollbacks are perfectly safe because an older image tag is guaranteed to run exactly as it did before.
* **No external dependencies at runtime**: The Spark pod doesn't need to connect to S3 or a PVC to fetch its code; it just starts running immediately.

### Cons
* **Slow CI pipeline**: Building and pushing a multi-gigabyte Docker image for a one-line Python fix can take minutes.
* **Storage cost**: Storing thousands of Docker images (one per commit) in a container registry can become expensive.

---

## 2. The "Fetch at Runtime" Strategy (S3/GCS)

In this approach, the Docker image only contains the Spark binaries and Python dependencies. The actual application code (`.py` files) lives in an object store (S3, MinIO, GCS) and is downloaded by the pod when it starts.

### How it works
1. **Commit**: You push changes to Git.
2. **Upload**: A fast CI pipeline copies your Python scripts to an S3 bucket, placing them in a versioned folder (e.g., `s3://my-bucket/releases/v1.2.3/`).
3. **Deploy**: Your orchestrator submits a `SparkApplication` that points `mainApplicationFile` to the S3 path.

### Example SparkApplication YAML
```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: daily-etl-v1-2-3
  namespace: spark-jobs
spec:
  # The image contains only Spark + pip dependencies, NOT the code.
  image: my-registry.com/spark-base:3.5.1
  
  # Spark pulls the main script directly from S3 at startup
  mainApplicationFile: s3a://my-bucket/releases/v1.2.3/jobs/daily_etl.py
  
  deps:
    # Spark downloads these extra files and places them in the pod's working directory
    pyFiles:
      - s3a://my-bucket/releases/v1.2.3/common.zip
```

*(Note: To use `s3a://` paths, the Spark image must have the `hadoop-aws` jars, and the pod must have the correct AWS credentials/IAM roles).*

### Pros
* **Blazing fast CI/CD**: Uploading a few kilobytes of Python files to S3 takes milliseconds. You only need to rebuild the Docker image when you add a new library to `requirements.txt` or upgrade Spark.
* **Storage efficiency**: Storing text files in S3 is practically free compared to storing Docker images.

### Cons
* **Separation of concerns**: The environment (Docker image) and the code (S3) are versioned separately. If a script in S3 relies on a pip package that isn't in the `spark-base` image, it will crash at runtime.
* **Startup latency**: The pod has to reach out to S3 to download the code before it can start executing.

---

## Summary & Recommendation

For modern data engineering teams writing **PySpark**:

**Use the "Fetch at Runtime" (S3) strategy.** 
Because Python is an interpreted language, you change the code frequently, but you change the underlying environment (pip packages, Spark version) rarely. Maintain a `spark-base` image that you update maybe once a month. Let your CI/CD pipeline zip your Python codebase and upload it to S3 on every merge to `main`. This gives you the best balance of speed, cost, and reliability.
