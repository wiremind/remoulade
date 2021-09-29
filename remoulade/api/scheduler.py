from functools import update_wrapper

import pytz
from flask import Blueprint, request
from marshmallow import Schema, ValidationError, fields, validates_schema
from marshmallow.validate import OneOf

from remoulade import get_scheduler
from remoulade.errors import NoScheduler
from remoulade.scheduler import ScheduledJob

scheduler_bp = Blueprint("scheduler", __name__, url_prefix="/scheduled")


class ScheduledJobSchema(Schema):
    """
    Class to validate scheduled jobs to add
    """

    actor_name = fields.Str()
    args = fields.List(fields.Raw(), allow_none=True)
    kwargs = fields.Dict(allow_none=True)
    interval = fields.Int(allow_none=True)
    daily_time = fields.Time(allow_none=True)
    iso_weekday = fields.Int(allow_none=True)
    enabled = fields.Bool(allow_none=True)
    last_queued = fields.DateTime(allow_none=True)
    tz = fields.Str(allow_none=True, validate=OneOf(pytz.all_timezones))

    @validates_schema
    def validate_sort(self, data, **kwargs):
        if data.get("daily_time") is not None and data.get("interval") != 86400:
            raise ValidationError("daily_time can only be used with 24h interval")


def with_scheduler(func):
    def wrapper(*args, **kwargs):
        try:
            scheduler = get_scheduler()
            func(scheduler, *args, **kwargs)
            scheduled_jobs = scheduler.get_redis_schedule()
            return {"result": [job.as_dict() for job in scheduled_jobs.values()]}

        except NoScheduler:
            return {"error": "this route needs a scheduler, but no scheduler was found."}, 400

    return update_wrapper(wrapper, func)


@scheduler_bp.route("/jobs")
@with_scheduler
def get_jobs(scheduler):
    pass


@scheduler_bp.route("/jobs", methods=["POST"])
@with_scheduler
def add_job(scheduler):
    job_dict = ScheduledJobSchema().load(request.json)
    scheduler.add_job(ScheduledJob(**job_dict))


@scheduler_bp.route("jobs/<job_hash>", methods=["DELETE"])
@with_scheduler
def delete_job(scheduler, job_hash):
    scheduler.delete_job(job_hash)


@scheduler_bp.route("/jobs/<job_hash>", methods=["PUT"])
@with_scheduler
def update_job(scheduler, job_hash):
    job_dict = ScheduledJobSchema().load(request.json)
    scheduler.delete_job(job_hash)
    scheduler.add_job(ScheduledJob(**job_dict))
