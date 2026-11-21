"""The zombie-run automation from Prefect's docs ("Detect zombie flows"), for a self-hosted
server: after a flow-run heartbeat, expect another flow-run event within 90 s, or mark the
run Crashed. The docs pair 90 s with a 30 s heartbeat (PREFECT_FLOWS_HEARTBEAT_FREQUENCY=30,
set in docker-compose.zombie.yml). Run it once inside the serving container:

  docker exec lab-prefect-etl-serve-1 python /opt/flow/zombie-automation.py
"""
from datetime import timedelta

from prefect.automations import Automation
from prefect.client.schemas.objects import StateType
from prefect.events.actions import ChangeFlowRunState
from prefect.events.schemas.automations import EventTrigger, Posture
from prefect.events.schemas.events import ResourceSpecification

automation = Automation(
    name="Crash zombie flows",
    trigger=EventTrigger(
        after={"prefect.flow-run.heartbeat"},
        expect={"prefect.flow-run.*"},
        match=ResourceSpecification({"prefect.resource.id": ["prefect.flow-run.*"]}),
        for_each={"prefect.resource.id"},
        posture=Posture.Proactive,
        threshold=1,
        within=timedelta(seconds=90),
    ),
    actions=[
        ChangeFlowRunState(
            state=StateType.CRASHED,
            message="Flow run marked as crashed due to missing heartbeats.",
        )
    ],
)

if __name__ == "__main__":
    print(automation.create())
