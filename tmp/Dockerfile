# Use a small Python image
FROM python:3.14-slim

# Set the working directory
WORKDIR /app

# Copy dependency list and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python script
COPY listener.py .

# Set default command
CMD ["python", "listener.py"]
