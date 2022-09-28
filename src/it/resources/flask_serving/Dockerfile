# thanks to https://blog.keras.io/building-a-simple-keras-deep-learning-rest-api.html
FROM python:3.6.8-slim-jessie

# install python packages
RUN pip install flask==1.1.1 gevent==1.5.0 requests==2.22.0 keras==2.2.5 tensorflow==1.13.2 pillow==5.4.0 grpcio==1.8.6 protobuf==3.19.5

# download model so it doesnt get downloaded each run (slow) as this model is ~100MB
RUN apt-get update && \
  apt-get -y install wget && \
  mkdir -p /root/.keras/models/ && \
  wget https://github.com/fchollet/deep-learning-models/releases/download/v0.2/resnet50_weights_tf_dim_ordering_tf_kernels.h5 -O /root/.keras/models/resnet50_weights_tf_dim_ordering_tf_kernels.h5

# copy in flask script
COPY run.py .

EXPOSE 5000

ENTRYPOINT [ "python", "run.py" ]