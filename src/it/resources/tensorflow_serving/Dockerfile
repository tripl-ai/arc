FROM tensorflow/serving:2.3.0

COPY models /models

EXPOSE 9001

ENTRYPOINT ["tensorflow_model_server", "--port=9000", "--rest_api_port=9001", "--model_name=simple", "--model_base_path=/models/simple_model"]
