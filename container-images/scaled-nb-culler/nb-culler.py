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



if __name__ == '__main__':
    logging.basicConfig(level='INFO')

    culler_dict = json.loads(os.environ["CULLER_DICT"])

    if not culler_dict:
        LOG.error('CULLER_DICT environment variables is required.')
        sys.exit(1)

    all_projects = sorted(p.model.metadata.name for p in oc.selector("projects").objects())
    class_info = get_class_ns(culler_dict, all_projects)
    namespace_processing(class_info)
