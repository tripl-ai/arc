FROM ubuntu:18.04

RUN apt-get update && apt-get install -y curl libcurl4 gnupg apt-utils \
  && echo "deb [arch=amd64] http://storage.googleapis.com/tensorflow-serving-apt stable tensorflow-model-server tensorflow-model-server-universal" | tee /etc/apt/sources.list.d/tensorflow-serving.list \
  && curl https://storage.googleapis.com/tensorflow-serving-apt/tensorflow-serving.release.pub.gpg | apt-key add - \
  && apt-get update && apt-get install tensorflow-model-server

COPY models /models

ENTRYPOINT ["tensorflow_model_server", "--port=9000", "--model_name=simple", "--model_base_path=/models/simple_model"]
