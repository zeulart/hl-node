
# Use an official Python runtime as a parent image
FROM python:3.9-slim

ARG USERNAME=hluser
ARG USER_UID=10000
ARG USER_GID=$USER_UID

# Create user and group
RUN groupadd --gid $USER_GID $USERNAME && \
    useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

# Set the working directory in the container
WORKDIR /home/$USERNAME/app

# Copy the application files to the container
COPY . /home/$USERNAME/app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Change ownership of the app directory
RUN chown -R $USERNAME:$USERNAME /home/$USERNAME/app

# Switch to the new user
USER $USERNAME

# Run main.py when the container launches
ENTRYPOINT ["python", "-u", "main.py"]
