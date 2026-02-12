from datetime import datetime, timezone
import openshift_client as oc
from typing import Optional
import logging
LOG = logging.getLogger(__name__)

def get_running_started_at(nb: dict) -> Optional[str]:
    """
    get time the container started running at
    """
    status = nb.get("status") or {}

    cs = status.get("containerState") or {}
    running = cs.get("running") or {}
    if running.get("startedAt"):
        return running.get("startedAt")

    return None

def parse_rfc3339(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def as_bool(v) -> bool:
    """
    Ensure strings passed in are normalized to booleans
    """
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in {"true", "1", "yes", "y"}
    return bool(v)

def get_notebook_username(nb: dict) -> Optional[str]:
    """return the notebook user from annotations, if present."""
    ann = (nb.get("metadata") or {}).get("annotations") or {}

    for k in ("opendatahub.io/username", "notebooks.opendatahub.io/username"):
        v = ann.get(k)
        if v:
            return str(v).strip()
    return None

def get_group_users(group_name: str) -> set[str]:
    """
    Get all users from an OpenShift group and return as a set.
    Equivalent to: oc get group $group_name -o=jsonpath='{.users[*]}'
    """
    try:
        group_sel = oc.selector(f"group/{group_name}")
        objs = list(group_sel.objects())
        if not objs:
            LOG.warning("Group %s not found", group_name)
            return set()

        group = objs[0].as_dict()
        users = group.get("users") or []
        return {str(u).strip() for u in users if u}

    except Exception as e:
        LOG.error("Failed to get users for group %s: %s", group_name, e)
        return set()
