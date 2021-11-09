from functools import update_wrapper

import pytz
from flask import Blueprint
from flask_apispec import doc, marshal_with
from marshmallow import Schema, ValidationError, fields, validates_schema
from marshmallow.validate import OneOf

import remoulade
from remoulade import get_scheduler
from remoulade.errors import NoScheduler
from remoulade.scheduler import ScheduledJob

from .apispec import validate_schema

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

    @validates_schema
    def validate_actor_name(self, data, **kwargs):
        actor_name = data.get("actor_name")
        if actor_name not in remoulade.get_broker().actors.keys():
            raise ValidationError(f"No actor named {actor_name} exists")

    @validates_schema
    def validate_tz(self, data, **kwargs):
        daily_time = data.get("daily_time")
        last_queued = data.get("last_queued")
        if (daily_time is not None and daily_time.tzinfo is not None) or (
            last_queued is not None and last_queued.tzinfo is not None
        ):
            raise ValidationError("Datetimes should be naive")


class JobWithHashSchema(ScheduledJobSchema):
    hash = fields.Str()


class ScheduleResponseSchema(Schema):
    result = fields.List(fields.Nested(JobWithHashSchema), allow_none=True)
    error = fields.Str(allow_none=True)


class ScheduledJobsSchema(Schema):
    """
    Class to validate multiple scheduled jobs at once
    """

    jobs = fields.Dict(keys=fields.Str(), values=fields.Nested(ScheduledJobSchema))


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
@doc(tags=["scheduler"])
@marshal_with(ScheduleResponseSchema)
@with_scheduler
def get_jobs(scheduler):
    pass


@scheduler_bp.route("/jobs", methods=["POST"])
@doc(tags=["scheduler"])
@marshal_with(ScheduleResponseSchema)
@validate_schema(ScheduledJobSchema)
@with_scheduler
def add_job(scheduler, **kwargs):
    scheduler.add_job(ScheduledJob(**kwargs))


@scheduler_bp.route("jobs/<job_hash>", methods=["DELETE"])
@doc(tags=["scheduler"])
@marshal_with(ScheduleResponseSchema)
@with_scheduler
def delete_job(scheduler, job_hash):
    scheduler.delete_job(job_hash)


@scheduler_bp.route("/jobs/<job_hash>", methods=["PUT"])
@doc(tags=["scheduler"])
@marshal_with(ScheduleResponseSchema)
@validate_schema(ScheduledJobSchema)
@with_scheduler
def update_job(scheduler, job_hash, **kwargs):
    scheduler.delete_job(job_hash)
    scheduler.add_job(ScheduledJob(**kwargs))


@scheduler_bp.route("/jobs", methods=["PUT"])
@doc(tags=["scheduler"])
@marshal_with(ScheduleResponseSchema)
@validate_schema(ScheduledJobsSchema)
@with_scheduler
def update_jobs(scheduler, **kwargs):
    for job_hash, job_dict in kwargs["jobs"].items():
        scheduler.delete_job(job_hash)
        scheduler.add_job(ScheduledJob(**job_dict))


scheduler_routes = [get_jobs, add_job, delete_job, update_job, update_jobs]
