# Use an existing base image
FROM circleci/python:3.8.9

# Install OpenJDK (you can specify the version you need)
RUN sudo apt-get update && sudo apt-get install -y openjdk-8-jre
