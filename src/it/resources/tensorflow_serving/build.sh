rm -rf models/simple_model/1/*
docker run --rm -it \
-v $(pwd):/src \
-w /src \
tensorflow/tensorflow:2.3.1 \
python3 simple.py