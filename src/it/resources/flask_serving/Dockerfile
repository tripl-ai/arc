# thanks to https://blog.keras.io/building-a-simple-keras-deep-learning-rest-api.html
FROM python:3.9

RUN apt-get update && \
  apt-get -y install wget build-essential


# install python packages
RUN pip install flask==1.1.4 gevent==23.9.1 requests==2.31.0 keras==2.15.0 tensorflow==2.15.0 pillow==10.2.0 grpcio==1.60.1 protobuf==3.20.3 markupsafe==2.0.1

# download model so it doesnt get downloaded each run (slow) as this model is ~100MB
RUN mkdir -p /root/.keras/models/ && \
  wget https://storage.googleapis.com/tensorflow/keras-applications/resnet/resnet50_weights_tf_dim_ordering_tf_kernels.h5 -O /root/.keras/models/resnet50_weights_tf_dim_ordering_tf_kernels.h5

# copy in flask script
COPY run.py .

EXPOSE 5000

ENTRYPOINT [ "python", "run.py" ]