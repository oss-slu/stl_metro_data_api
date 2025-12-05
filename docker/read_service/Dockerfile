# Docker file for the read service Python app
FROM python:3.13-slim

# Copy project files to Docker container
COPY src/ src/
COPY requirements.txt .

# I will use port 5001
EXPOSE 5001

# Install required Python libraries and then start the app
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "-m", "src.read_service.app"]
