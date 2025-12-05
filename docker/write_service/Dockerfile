# Docker file for the write service Python app
FROM python:3.13-slim

# Copy project files to Docker container
COPY src/ src/
COPY requirements.txt .

# I will use port 5000
EXPOSE 5000

# Install required Python libraries and then start the app
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "-m", "src.write_service.app"]