"""Mark Prefect flow runs that are Running or Cancelling but have no process behind them as
Crashed. This is the state change the zombie-detection automation in the Prefect docs makes."""
import asyncio
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterState, FlowRunFilterStateType
from prefect.states import Crashed

async def main():
    async with get_client() as c:
        f = FlowRunFilter(state=FlowRunFilterState(type=FlowRunFilterStateType(any_=["RUNNING", "CANCELLING"])))
        for r in await c.read_flow_runs(flow_run_filter=f):
            res = await c.set_flow_run_state(r.id, Crashed(message="marked crashed by hand after host reboot"), force=True)
            print(r.id, r.state.name, "->", res.status, res.state.name if res.state else None)

asyncio.run(main())
