# Use Miniconda3 as the base image
FROM --platform=linux/amd64 python:3.9.20-slim

RUN apt update
RUN apt install make wget curl -y

# Setup miniconda - seems to be a requirement
RUN mkdir -p ~/miniconda3
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
RUN bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
RUN rm ~/miniconda3/miniconda.sh

# Set the working directory in the container
WORKDIR /app

COPY environment.yml ./

# Copy the current directory contents into the container at /app
COPY . .

RUN make install-poetry
RUN make setup_admin_app

# Command allowing exploration of the container
# CMD [ "tail", "-f", "/dev/null" ] 
# Run command to start your application, replace with actual command if different
CMD ["make", "run_admin_app"]