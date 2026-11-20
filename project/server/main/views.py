import os
import requests
import redis

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue
from project.server.main.tasks import (ETAPES, HORS_DEFAUT, PAR_DEFAUT, create_task_compute,
                                       create_task_projects, etapes_choisies, snapshots_effectifs)
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


@main_blueprint.route("/compute", methods=["GET"])
def list_steps_compute():
    """Étapes disponibles pour POST /compute (voir tasks.ETAPES)."""
    return jsonify({"etapes": ETAPES, "par_defaut": PAR_DEFAUT, "hors_defaut": HORS_DEFAUT,
                    "snapshots_defaut": snapshots_effectifs({})})


@main_blueprint.route("/compute", methods=["POST"])
def run_task_compute():
    """Corps JSON : {} (chaîne complète), {"steps": [...]}, {"start": ..., "end": ...},
    "params": {étape: {argument: valeur}}, "snapshot" et "snapshot_awards": "AAAA-MM-JJ",
    "jusqu_a_complet": true (horizon_topics / anr relancées chaque jour à 06:00 UTC jusqu'à
    « complet », au plus "relances_max" fois) (voir tasks.py)."""
    args = request.get_json(force=True, silent=True) or {}
    try:
        etapes = etapes_choisies(args)
        snapshots = snapshots_effectifs(args)
    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="athena", default_timeout=default_timeout)
        task = q.enqueue(create_task_compute, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id(), "etapes": etapes,
                                                     "snapshots": snapshots}}
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
