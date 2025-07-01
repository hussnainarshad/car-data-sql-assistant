# Dockerfile for FastAPI
FROM python:3.11

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the FastAPI application files
COPY . .

# Copy the shell script
COPY run_scripts.sh .

# Make the shell script executable and give all permissions
RUN chmod 777 run_scripts.sh

# Expose the port the app runs on
EXPOSE 8000

# Command to run the shell script
CMD ["./run_scripts.sh"]

