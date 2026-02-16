import os
import sys
import openshift_client as oc
import logging
import json
from typing import TypedDict
from datetime import datetime, timezone
from helpers import (
    get_running_started_at,
    parse_rfc3339,
    as_bool,
    get_notebook_username,
    get_group_users,
)

LOG = logging.getLogger(__name__)

def get_class_ns(culler: dict, all_projects: list[str]) -> tuple[dict, dict]:
    multi_ns: dict[str, dict] = {}
    single_ns: dict[str, dict] = {}

    for class_name, config in culler.items():
        cutoff = int(config["cutoff"])
        ns = config["ns"]
        mult_ns = as_bool(config.get("multiple-ns", False))

        if mult_ns:
            namespaces = [p for p in all_projects if p.startswith(ns)]
            multi_ns[class_name] = {
                "cutoff": cutoff,
                "namespaces": namespaces,
            }
        else:
            single_ns[class_name] = {
                "cutoff": cutoff,
                "namespace": ns,
            }

    return multi_ns, single_ns


def stop_notebook(nb: dict, namespace: str, cutoff_seconds: int) -> bool:
    """ patch notebook if past cutoff, returns true if stopped nb, and false otherwise"""

    started_at = get_running_started_at(nb)
    if not started_at:
        return False

    start_dt = parse_rfc3339(started_at)
    age_seconds = int((datetime.now(timezone.utc) - start_dt).total_seconds())
    if age_seconds <= cutoff_seconds:
        LOG.info("Notebook found, but within cutoff. (age=%ss < cutoff=%ss)", age_seconds, cutoff_seconds)
        return False

    nb_name = (nb.get("metadata") or {}).get("name")
    if not nb_name:
        return False

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    patch_obj = {
        "metadata": {
            "annotations": {
                "kubeflow-resource-stopped": now_utc,
            }
        }
    }

    #oc.invoke("patch", ["notebook", nb_name, "-n", namespace, "--type=merge", "-p", json.dumps(patch_obj)])
    LOG.info("Patched notebook %s/%s (age=%ss > cutoff=%ss) with kubeflow-resource-stopped=%s",
             namespace, nb_name, age_seconds, cutoff_seconds, now_utc)

    return True


def namespace_processing(class_info: tuple[dict, dict]) -> None:
    """
    cull per user for single namespace classes
    cull based on time based on multi namespace classes
    """
    LOG.info("we get here")
    multi_ns, single_ns = class_info

    process_single(single_ns)

    for class_name, config in multi_ns.items():
        cutoff = config["cutoff"]
        namespaces = config["namespaces"]
        LOG.info("Processing multi-ns class %s: %d namespace(s), cutoff=%ss", class_name, len(namespaces), cutoff)
        for namespace in namespaces:
            LOG.info("Checking namespace %s", namespace)
            try:
                with oc.project(namespace):
                    notebooks = oc.selector("notebooks").objects()
                    running = [nb.as_dict() for nb in notebooks if get_running_started_at(nb.as_dict())]

                    if not running:
                        LOG.info("No running notebooks in %s", namespace)
                    else:
                        for nb in running:
                            stop_notebook(nb, namespace, cutoff)

            except Exception as e:
                LOG.error("Error processing namespace %s: %s", namespace, e)

def process_single(single_ns: dict[str, dict]) -> None:
    """
    All single-namespace classes share the same namespace.
    """
    if not single_ns:
        return

    # shared namespace (assumed identical across all entries)
    namespace = next(iter(single_ns.values()))["namespace"]

    LOG.info(
        "Processing shared single namespace %s for %d class(es)",
        namespace,
        len(single_ns),
    )

    try:
        # buyild a lookup for faster parsing
        user_to_info: dict[str, dict] = {}

        for class_name, config in single_ns.items():
            cutoff = int(config["cutoff"])
            users = get_group_users(class_name)

            LOG.info("Loaded group %s (%d users, cutoff=%ss)", class_name, len(users), cutoff)

            for u in users:
                u = str(u).strip()
                if not u:
                    continue
                if u in user_to_info:
                    prev = user_to_info[u]
                    if cutoff > prev["cutoff"]:
                        LOG.warning(
                            "User %s in multiple groups (%s, %s). Using more lenient cutoff %ss from %s.",
                            u, prev["class"], class_name, cutoff, class_name
                        )
                        user_to_info[u] = {"class": class_name, "cutoff": cutoff}
                else:
                    user_to_info[u] = {"class": class_name, "cutoff": cutoff}

        LOG.info("Built user_to_info map with %d total users", len(user_to_info))

        # assume all the classes run in one project (rhods-notebooks)
        with oc.project(namespace):
            notebooks = oc.selector("notebooks").objects()

            for nb_obj in notebooks:
                nb = nb_obj.as_dict()

                if not get_running_started_at(nb):
                    continue

                username = get_notebook_username(nb)
                if not username:
                    LOG.warning(
                        "Notebook %s missing username annotation — skipping",
                        (nb.get("metadata") or {}).get("name"),
                    )
                    continue

                info = user_to_info.get(username)
                if not info:
                    LOG.info("DELETE USER HERE")
                    continue

                cutoff = info["cutoff"]
                class_name = info["class"]

                LOG.info(
                    "Notebook user=%s matched class=%s (cutoff=%ss)",
                    username,
                    class_name,
                    cutoff,
                )

                stop_notebook(nb, namespace, cutoff)

    except Exception as e:
        LOG.error("Error processing shared single namespace %s: %s", namespace, e)


if __name__ == '__main__':
    logging.basicConfig(level='INFO')

    culler_dict = json.loads(os.environ["CULLER_DICT"])

    if not culler_dict:
        LOG.error('CULLER_DICT environment variables is required.')
        sys.exit(1)

    all_projects = sorted(p.model.metadata.name for p in oc.selector("projects").objects())
    class_info = get_class_ns(culler_dict, all_projects)
    namespace_processing(class_info)
