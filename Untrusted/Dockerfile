# Use an official Python runtime as a parent image
FROM python:3.6.8-slim

# Set the working directory to /app
WORKDIR /app

RUN useradd -r -u 1001 untrusted
RUN awk -F: '($3 >= 1000) {printf "%s:%s\n",$1,$3}' /etc/passwd

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Define environment variable
ENV NAME World

# Run app.py when the container launches
CMD ["python", "-u", "app.py"]

