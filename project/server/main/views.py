import os
import requests
import redis

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue
from project.server.main.tasks import create_task_compute, create_task_projects
from project.server.main.logger import get_logger

default_timeout = 4320000

logger = get_logger(__name__)

main_blueprint = Blueprint(
    "main",
    __name__,
)


@main_blueprint.route("/", methods=["GET"])
def home():
    return render_template("home.html")


@main_blueprint.route("/compute", methods=["POST"])
def run_task_compute():
    args = request.get_json(force=True)

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="athena", default_timeout=default_timeout)
        task = q.enqueue(create_task_compute, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route("/projects", methods=["POST"])
def run_task_projects():
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="athena", default_timeout=default_timeout)
        task = q.enqueue(create_task_projects)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route("/tasks/<task_id>", methods=["GET"])
def get_status(task_id):
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("athena")      # même file que /compute (était "sandbox" : tâche jamais trouvée)
        task = q.fetch_job(task_id)
    if task:
        response_object = {
            "status": "success",
            "data": {
                "task_id": task.get_id(),
                "task_status": task.get_status(),
                "task_result": task.result,
            },
        }
    else:
        response_object = {"status": "error"}
    return jsonify(response_object)
