import os
import sys
import openshift_client as oc
import logging
import json
from datetime import datetime, timezone
from helpers import (
    parse_rfc3339,
    as_bool,
    build_user_cutoff_map,
    get_running_notebooks,
    get_notebook_username_map,
)

LOG = logging.getLogger(__name__)


def get_class_ns(culler: dict) -> tuple[dict, dict]:
    """
    build multi ns and single ns dicts
    """
    multi_ns: dict[str, dict] = {}
    single_ns: dict[str, dict] = {}

    for class_name, config in culler.items():
        cutoff = int(config["cutoff"])
        ns = config["ns"]
        mult_ns = as_bool(config.get("multiple-ns", False))

        if mult_ns:
            multi_ns[class_name] = {
                "cutoff": cutoff,
                "prefix": ns,
            }
        else:
            single_ns[class_name] = {
                "cutoff": cutoff,
                "namespace": ns,
            }

    return multi_ns, single_ns


def stop_notebook(nb_name: str, started_at: str, namespace: str, cutoff_seconds: int) -> bool:
    """ Patch notebook if past cutoff. Returns True if stopped. """
    start_dt = parse_rfc3339(started_at)
    age_seconds = int((datetime.now(timezone.utc) - start_dt).total_seconds())

    if age_seconds <= cutoff_seconds:
        LOG.info(
            "Notebook %s/%s within cutoff (age=%ss < cutoff=%ss)",
            namespace, nb_name, age_seconds, cutoff_seconds
        )
        return False

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    patch_obj = {
        "metadata": {
            "annotations": {"kubeflow-resource-stopped": now_utc}
        }
    }

    try:
        oc.invoke("patch", ["notebook", nb_name, "-n", namespace, "--type=merge", "-p", json.dumps(patch_obj)])
        LOG.info("Patched notebook %s/%s (age=%ss > cutoff=%ss)", namespace, nb_name, age_seconds, cutoff_seconds)
        return True
    except Exception as e:
        LOG.error("Failed to patch notebook %s/%s: %s", namespace, nb_name, e)
        return False


def process_single(single_ns: dict[str, dict]) -> None:
    if not single_ns:
        LOG.info("No single namespace classes configured")
        return

    namespace = next(iter(single_ns.values()))["namespace"]

    LOG.info("Processing shared single namespace %s for %d class(es)", namespace, len(single_ns))

    try:
        user_to_info = build_user_cutoff_map(single_ns)

        with oc.project(namespace):
            running = get_running_notebooks(namespace)
            username_map = get_notebook_username_map(namespace)

            for nb in running:
                nb_name = nb["name"]

                username = username_map.get(nb_name)
                if not username:
                    LOG.warning("Notebook %s missing username annotation, skipping", nb_name)
                    continue

                info = user_to_info.get(username)
                if not info:
                    try:
                        pvc = f"jupyterhub-nb-{nb_name.removeprefix('jupyter-nb-')}-pvc"
                        oc.invoke("delete", ["notebook", nb_name, "-n", namespace])
                        oc.invoke("delete", ["pvc", pvc, "-n", namespace])

                        LOG.info("Deleted notebook %s and pvc %s in namespace %s", nb_name, pvc, namespace)
                    except Exception as e:
                        LOG.error("Failed deleting notebook %s or pvc in namespace %s: %s", nb_name,namespace, e)
                    continue

                stop_notebook(nb_name, nb["startedAt"], namespace, info["cutoff"])

    except Exception as e:
        LOG.error("Error processing shared single namespace %s: %s", namespace, e)


def process_multi(multi_ns: dict[str, dict]) -> None:
    matchers: list[tuple[str, int]] = []
    for class_name, cfg in multi_ns.items():
        prefix = str(cfg["prefix"]).strip()
        cutoff = int(cfg["cutoff"])
        if prefix:
            matchers.append((prefix, cutoff))

    if not matchers:
        LOG.info("No multi-namespace classes configured")
        return

    matchers.sort(key=lambda x: len(x[0]), reverse=True)

    try:
        running_all = get_running_notebooks()
    except Exception as e:
        LOG.error("Failed to list running notebooks across all namespaces: %s", e)
        return

    matched = 0
    for nb in running_all:
        ns = nb["namespace"]

        cutoff = None
        for prefix, c in matchers:
            if ns == prefix or ns.startswith(prefix + "-"):
                cutoff = c if cutoff is None else max(cutoff, c)

        if cutoff is None:
            continue

        matched += 1
        stop_notebook(nb["name"], nb["startedAt"], ns, cutoff)

    LOG.info("Matched %d running notebooks in multi namespaces", matched)


if __name__ == '__main__':
    logging.basicConfig(level='INFO')

    culler_dict = json.loads(os.environ["CULLER_DICT"])

    if not culler_dict:
        LOG.error('CULLER_DICT environment variables is required.')
        sys.exit(1)

    class_info = get_class_ns(culler_dict)
    multi_ns, single_ns = class_info

    process_single(single_ns)
    process_multi(multi_ns)
