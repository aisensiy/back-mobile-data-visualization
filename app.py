from flask import Flask
from flask.ext.cors import CORS

app = Flask(__name__)
cors = CORS(app)


@app.route("/")
def hello():
    return "Hello World!"


if __name__ == "__main__":
    app.run()
