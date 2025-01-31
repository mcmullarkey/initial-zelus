# Use rocker/tidyverse as base image
FROM rocker/tidyverse:4.4.2 as builder

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

# Make scripts executable
RUN chmod +x scripts/*

# Run the training pipeline to generate model
RUN Rscript R/train.R

# Start fresh for the final image to reduce size
FROM rocker/tidyverse:4.4.2

WORKDIR /app

# Copy only the necessary files from builder
COPY --from=builder /app/models /app/models
COPY --from=builder /app/data/processed /app/data/processed
COPY R/predict.R /app/R/
COPY scripts/predict.sh /app/scripts/

# Make predict script executable
RUN chmod +x scripts/predict.sh

# Set default command to show usage
ENTRYPOINT ["/app/scripts/predict.sh"]