# Base image - use Databricks minimal runtime as the base image
FROM databricksruntime/minimal:15.4-LTS

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies, upgrade pip, and clean up in a single step (Python, Supervisor, Java)
RUN apt-get update && apt-get install -y \
    python3-pip \
    openjdk-11-jdk && \
    pip install --upgrade pip && \
    rm -rf /var/lib/apt/lists/*

# Add cleanup cron job - creates a cron job that runs every 15 minutes & deletes files in /tmp older than 1 day
RUN echo "*/15 * * * * find /tmp -type f -mtime +1 -delete" >> /etc/crontab

# Set environment variables for Java (JAVA_HOME for Java 11)
ENV JAVA_HOME="/usr/lib/jvm/java-1.11.0-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Resource management
ENV MAX_WORKERS=4
ENV MEMORY_LIMIT="4g"
ENV DB_POOL_SIZE=10
ENV DB_POOL_TIMEOUT=30
ENV JUPYTER_CACHE_DIR="/tmp/jupyter_cache"

# Create Jupyter cache directory
RUN mkdir -p ${JUPYTER_CACHE_DIR}

# Copy the requirements file first (for better Docker caching)
# Install dependencies & requirements
COPY Docker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files and data into the container
COPY src/ /app/src/
COPY data/ /app/data/

# Set Python path to include src folder
ENV PYTHONPATH="/app/src:${PYTHONPATH}"

# Define a mountable volume for persistent data (optional)
# This can be removed if wants to mount the volume manually
VOLUME ["/app/data", "/app/logs"]


# Expose both Jupyter Notebook ports
EXPOSE 8888

# Command to start the Jupyter Notebook server.
# --ip=0.0.0.0 to listen on all interfaces, --allow-root allows the container user to run as root, and
# --NotebookApp.token='' disables token authentication (for development purposes).
# command=jupyter notebook --ip 0.0.0.0 --port 8888 --allow-root --NotebookApp.max_buffer_size=100000000 --NotebookApp.token='' --NotebookApp.password=''
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--NotebookApp.token=''"]
