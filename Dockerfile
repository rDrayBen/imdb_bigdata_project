ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.10.2

# Stage 1: Python environment
FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3

# Stage 2: Java environment with Python
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

# Copy Python environment from stage 1
COPY --from=py3 / /

ARG PYSPARK_VERSION=3.2.0

# Install PySpark and Jupyter
RUN pip --no-cache-dir install pyspark==${PYSPARK_VERSION} jupyter

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Expose Jupyter port
EXPOSE 8888

# Default command: Allow running either Jupyter Notebook or Python scripts
ENTRYPOINT ["bash", "-c"]
CMD ["jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root"]