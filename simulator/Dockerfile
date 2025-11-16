# 📁 simulator/Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Copy and install requirements
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the simulator code
COPY iot_simulator.py ./

# Define the command to run the simulator script
CMD ["python", "iot_simulator.py"]