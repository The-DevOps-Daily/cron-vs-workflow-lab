"""Setup 5: the same four steps as a Prefect flow, served by a long-running process."""

import argparse
from datetime import timedelta

from prefect import flow, runtime, task
from prefect.client.schemas.objects import ConcurrencyLimitConfig, ConcurrencyLimitStrategy

import etl


def run_step(name, partition, workdir, **opts):
    args = argparse.Namespace(setup="prefect", partition=partition, workdir=workdir, **opts)
    etl.event(args, name, "start")
    try:
        out = etl.STEPS[name](args)
    except Exception as e:
        etl.event(args, name, "error", error=f"{type(e).__name__}: {e}"[:300])
        raise
    etl.event(args, name, "ok", **out)
    return out


@task(retries=2, retry_delay_seconds=10, timeout_seconds=240)
def step(name, partition, workdir, **opts):
    return run_step(name, partition, workdir, **opts)


@flow(name="etl")
def etl_flow(partition: str | None = None):
    etl.RUN_ID = str(runtime.flow_run.id)
    if partition is None:
        # the scheduled time, not the clock: a late run still loads its own partition
        scheduled = runtime.flow_run.scheduled_start_time
        partition = (scheduled - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M")
    workdir = f"/work/{runtime.flow_run.id}"
    import os
    os.makedirs(workdir, exist_ok=True)
    step("extract", partition, workdir)
    step("transform", partition, workdir)
    step("load", partition, workdir, mode="upsert")
    step("report", partition, workdir, mode="once")


if __name__ == "__main__":
    etl_flow.serve(
        name="nightly",
        cron="* * * * *",
        # one run at a time for this deployment, manual runs included; extra runs wait
        global_limit=ConcurrencyLimitConfig(limit=1, collision_strategy=ConcurrencyLimitStrategy.ENQUEUE),
    )
