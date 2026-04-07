from datetime import datetime, timezone
import openshift_client as oc
from typing import Optional
import logging
import json
import sys

LOG = logging.getLogger(__name__)


def build_user_cutoff_map(single_ns: dict[str, dict]) -> dict[str, dict]:
    """Build a user -> {class, cutoff} lookup from all single-namespace class configs.
       If a user belongs to multiple groups, the more lenient (higher) cutoff wins.
    """
    user_to_info: dict[str, dict] = {}

    for class_name, config in single_ns.items():
        cutoff = int(config["cutoff"])
        users = get_group_users(class_name)

        LOG.info("Loaded group %s (%d users, cutoff=%ss)", class_name, len(users), cutoff)

        for u in users:
            u = str(u).strip()
            if not u:
                continue

            existing = user_to_info.get(u)
            if existing:
                if cutoff > existing["cutoff"]:
                    LOG.warning(
                        "User %s in multiple groups (%s, %s). Using more lenient cutoff %ss from %s.",
                        u, existing["class"], class_name, cutoff, class_name,
                    )
                    user_to_info[u] = {"class": class_name, "cutoff": cutoff}
            else:
                user_to_info[u] = {"class": class_name, "cutoff": cutoff}

    LOG.info("Built user_to_info map with %d total users", len(user_to_info))
    return user_to_info


def parse_rfc3339(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def as_bool(v) -> bool:
    """Ensure strings passed in are normalized to booleans"""
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
    "get users from the class group"
    try:
        result = oc.invoke("get", ["group", group_name, "-o", "json"])
        group = json.loads(result.out())
        if not group.get("users"):
            LOG.warning("Group %s is empty. This could lead to notebooks being deleted.", group_name)
        return set(group.get("users") or [])
    except Exception as e:
        LOG.error("Failed to get users for group %s: %s", group_name, e)
        return set()

def get_running_notebooks(namespace: Optional[str] = None) -> list[dict]:
    """
    Return running notebooks. If namespace is given, scope to that namespace;
    otherwise query all namespaces and include a 'namespace' key in each result.
    """
    if namespace:
        ns_args = ["-n", namespace]
        jsonpath = (
            '{range .items[?(@.status.containerState.running)]}'
            '{.metadata.name}{"\\t"}{.status.containerState.running.startedAt}{"\\n"}{end}'
        )
    else:
        ns_args = ["-A"]
        jsonpath = (
            '{range .items[?(@.status.containerState.running)]}'
            '{.metadata.namespace}{"\\t"}{.metadata.name}{"\\t"}'
            '{.status.containerState.running.startedAt}{"\\n"}{end}'
        )

    result = oc.invoke("get", ["notebooks"] + ns_args + ["-o", f"jsonpath={jsonpath}"])

    notebooks = []
    for line in result.out().strip().splitlines():
        parts = line.strip().split("\t")
        if namespace and len(parts) == 2:
            notebooks.append({"name": parts[0], "startedAt": parts[1]})
        elif not namespace and len(parts) == 3:
            notebooks.append({"namespace": parts[0], "name": parts[1], "startedAt": parts[2]})
    return notebooks


def get_notebook_username_map(namespace: str) -> dict[str, str]:
    """
    Fetch all notebooks in a namespace and return a
    name -> username map derived from annotations.
    """
    result = oc.invoke("get", ["notebooks", "-n", namespace, "-o", "json"])
    items = json.loads(result.out()).get("items", [])
    return {
        item["metadata"]["name"]: username
        for item in items
        if (username := get_notebook_username(item))
    }
