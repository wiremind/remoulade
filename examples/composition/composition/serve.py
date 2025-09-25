# Here, we run the remoulade API server written in Flask, which can be used to manage
# the broker or to get stats (through the super-bowl frontend)

from remoulade.api import app

from .actors import broker  # noqa: F401


# Run backend API server (used for superbowl for instance)
def serve():
    app.run(debug=True, host="0.0.0.0", port=5000, reloader_type="stat")  # noqa: S104
