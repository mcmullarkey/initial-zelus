# Use rocker/tidyverse as base image
FROM rocker/tidyverse:4.4.2 AS builder

# System dependencies for R packages
RUN apt-get update && apt-get install -y \
    libgit2-dev \
    libglpk-dev \
    libnode-dev \
    && rm -rf /var/lib/apt/lists/*

# Create app directory and set working directory
WORKDIR /app

# Copy renv files first for caching
COPY renv.lock renv.lock
COPY .Rprofile .Rprofile
COPY renv/activate.R renv/activate.R

# Initialize renv and restore packages
RUN Rscript -e 'install.packages("renv")' \
    && Rscript -e 'renv::restore()'

# Copy all application files
COPY R/ R/
COPY data/ data/
COPY scripts/ scripts/
COPY tests/ tests/

# Make scripts executable
RUN chmod +x scripts/*

# Run the training pipeline to generate model
RUN Rscript R/train.R

# Extract a minimal lockfile for prediction dependencies
RUN Rscript -e 'renv::dependencies("R/predict.R")$Package |> writeLines("packages.txt")' \
    && Rscript -e 'renv::snapshot(lockfile = "renv.lock.predict", packages = readLines("packages.txt"))'

# Stage 2: Slim Runtime with Only Necessary Packages
FROM rocker/tidyverse:4.4.2

WORKDIR /app

# Install renv and restore only prediction dependencies
COPY --from=builder /app/renv.lock.predict /app/renv.lock
RUN Rscript -e 'install.packages("renv")' \
    && Rscript -e 'renv::restore()'

# Copy only necessary files from builder stage
COPY --from=builder /app/models /app/models
COPY --from=builder /app/data/processed /app/data/processed
COPY --from=builder /app/logs /app/logs
COPY R/predict.R /app/R/
COPY scripts/predict.sh /app/scripts/

# Make predict script executable
RUN chmod +x scripts/predict.sh

# Default command
ENTRYPOINT ["/bin/bash"]