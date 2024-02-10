# USAGE
# Start the server:
# 	python run_keras_server.py
# Submit a request via cURL:
# 	curl -X POST -F image=@dog.jpg 'http://localhost:5000/predict'
# Submita a request via Python:
#	python simple_request.py

# import the necessary packages
import keras
from keras.applications.resnet50 import ResNet50
from keras.applications.resnet50 import preprocess_input, decode_predictions
from keras.preprocessing.image import img_to_array
from keras.applications import imagenet_utils
from PIL import Image
import numpy as np
import flask
import io
# import tensorflow as tf
# import tensorflow.compat.v1 as tf

# initialize our Flask application and the Keras model
app = flask.Flask(__name__)
model = None

def load_model():
	# load the pre-trained Keras model (here we are using a model
	# pre-trained on ImageNet and provided by Keras, but you can
	# substitute in your own networks just as easily)
	global model
	model = ResNet50(weights="imagenet")
	# global graph
	# graph = tf.get_default_graph()


def prepare_image(image, target):
	# if the image mode is not RGB, convert it
	if image.mode != "RGB":
		image = image.convert("RGB")

	# resize the input image and preprocess it
	image = image.resize(target)
	image = keras.utils.img_to_array(image)
	image = np.expand_dims(image, axis=0)
	image = preprocess_input(image)

	# return the processed image
	return image


@app.route("/predict", methods=["POST"])
def predict():
	# initialize the data dictionary that will be returned from the
	# view
	data = {"success": False}
	status = 400

	# ensure an image was properly uploaded to our endpoint
	if flask.request.method == "POST":

		# read the image in PIL format
		image = flask.request.get_data()
		image = Image.open(io.BytesIO(image))

		# preprocess the image and prepare it for classification
		image = prepare_image(image, target=(224, 224))

		# classify the input image and then initialize the list
		# of predictions to return to the client
		# with graph.as_default():
		preds = model.predict(image)
		results = imagenet_utils.decode_predictions(preds)
		data["predictions"] = []

		# loop over the results and add them to the list of
		# returned predictions
		for (imagenetID, label, prob) in results[0]:
			r = {"label": label, "probability": float(prob)}
			data["predictions"].append(r)

		# indicate that the request was a success
		data["success"] = True
		status = 200

	# return the data dictionary as a JSON response
	return flask.jsonify(data), status


# if this is the main thread of execution first load the model and
# then start the server
if __name__ == "__main__":
	print(("* Loading Keras model and Flask starting server..."
		   "please wait until server has fully started"))
	load_model()
	app.run(host='0.0.0.0')
