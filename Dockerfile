# 1. Base image
FROM python:3.10-slim

# 2. Prevent Python from writing .pyc files & enable logs immediately
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. Set working directory inside container
WORKDIR /app

# 4. Install system dependencies (useful for pandas, numpy, etc.)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 5. Copy only requirements first (better caching)
COPY requirements.txt /app/

# 6. Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 7. Copy the rest of the project
COPY . /app

# 8. Default command to run your feature builder pipeline
CMD ["python", "main.py"]