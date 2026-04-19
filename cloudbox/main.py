"""Cloudbox main entry point.

Starts one uvicorn server per service, each on its own port, plus the
admin UI. All servers run concurrently via asyncio.

Pub/Sub runs two servers:
  • gRPC on pubsub_port  (8085) — compatible with PUBSUB_EMULATOR_HOST
  • REST on pubsub_rest_port (8086) — for transport="rest" SDK clients
"""

import asyncio
import logging
import os
import signal

import uvicorn

from cloudbox.config import settings

logger = logging.getLogger("cloudbox")

# Services and their metadata — (name, label, port_getter)
_SERVICES = [
    ("gcs", "Cloud Storage", lambda: settings.gcs_port),
    ("pubsub_rest", "Cloud Pub/Sub REST", lambda: settings.pubsub_rest_port),
    ("firestore", "Cloud Firestore", lambda: settings.firestore_port),
    ("secretmanager", "Secret Manager", lambda: settings.secretmanager_port),
    ("tasks", "Cloud Tasks", lambda: settings.tasks_port),
    ("bigquery", "BigQuery", lambda: settings.bigquery_port),
    ("spanner", "Cloud Spanner", lambda: settings.spanner_port),
    ("logging", "Cloud Logging", lambda: settings.logging_port),
    ("scheduler", "Cloud Scheduler", lambda: settings.scheduler_port),
    ("admin", "Admin UI", lambda: settings.admin_port),
]


def _build_configs() -> list[tuple[str, uvicorn.Config]]:
    """Build a uvicorn Config for every registered service.

    Returns:
        list[tuple[str, uvicorn.Config]]: Pairs of (service name, uvicorn Config)
            in the same order as _SERVICES.
    """
    from cloudbox.admin.app import app as admin_app
    from cloudbox.services.bigquery.app import app as bigquery_app
    from cloudbox.services.firestore.app import app as firestore_app
    from cloudbox.services.gcs.app import app as gcs_app
    from cloudbox.services.logging.app import app as logging_app
    from cloudbox.services.pubsub.app import app as pubsub_app
    from cloudbox.services.scheduler.app import app as scheduler_app
    from cloudbox.services.secretmanager.app import app as secretmanager_app
    from cloudbox.services.spanner.app import app as spanner_app
    from cloudbox.services.tasks.app import app as tasks_app

    apps = {
        "gcs": gcs_app,
        "pubsub_rest": pubsub_app,
        "firestore": firestore_app,
        "secretmanager": secretmanager_app,
        "tasks": tasks_app,
        "bigquery": bigquery_app,
        "spanner": spanner_app,
        "logging": logging_app,
        "scheduler": scheduler_app,
        "admin": admin_app,
    }

    log_level = os.environ.get("CLOUDBOX_LOG_LEVEL", "info").lower()

    configs = []
    for name, _label, port_fn in _SERVICES:
        log_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "()": "uvicorn.logging.DefaultFormatter",
                    "fmt": f"%(asctime)s  cloudbox.{name:<14s}  %(levelprefix)s %(message)s",
                    "use_colors": None,
                },
                "access": {
                    "()": "uvicorn.logging.AccessFormatter",
                    "fmt": (
                        f"%(asctime)s  cloudbox.{name:<14s}  %(levelprefix)s"
                        ' %(client_addr)s - "%(request_line)s" %(status_code)s'
                    ),
                },
            },
            "handlers": {
                "default": {
                    "formatter": "default",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stderr",
                },
                "access": {
                    "formatter": "access",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                },
            },
            "loggers": {
                "uvicorn": {
                    "handlers": ["default"],
                    "level": log_level.upper(),
                    "propagate": False,
                },
                "uvicorn.error": {"level": log_level.upper(), "propagate": True},
                "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
            },
        }
        configs.append(
            (
                name,
                uvicorn.Config(
                    app=apps[name],
                    host=settings.host,
                    port=port_fn(),
                    log_level=log_level,
                    access_log=True,
                    log_config=log_config,
                ),
            )
        )
    return configs


async def _serve_all(configs: list[tuple[str, uvicorn.Config]]) -> None:
    """Start all HTTP servers, the Pub/Sub gRPC server, and the Scheduler worker.

    Args:
        configs (list[tuple[str, uvicorn.Config]]): Service name / uvicorn Config
            pairs as returned by _build_configs.
    """
    servers = [uvicorn.Server(cfg) for _, cfg in configs]

    # Build and start the Pub/Sub gRPC server
    from cloudbox.services.pubsub.grpc_server import create_server as create_pubsub_grpc
    from cloudbox.services.scheduler.worker import dispatch_loop as scheduler_loop

    grpc_server = await create_pubsub_grpc(settings.host, settings.pubsub_port)

    loop = asyncio.get_running_loop()
    scheduler_task: asyncio.Task | None = None

    def _shutdown(*_):
        logger.info("Shutting down Cloudbox …")
        for s in servers:
            s.should_exit = True
        asyncio.create_task(grpc_server.stop(grace=5))
        if scheduler_task is not None:
            scheduler_task.cancel()

    loop.add_signal_handler(signal.SIGINT, _shutdown)
    loop.add_signal_handler(signal.SIGTERM, _shutdown)

    await grpc_server.start()
    scheduler_task = asyncio.create_task(scheduler_loop())

    await asyncio.gather(
        *[s.serve() for s in servers],
        grpc_server.wait_for_termination(),
        scheduler_task,
        return_exceptions=True,
    )


def main() -> None:
    """Entry point: configure logging and start all Cloudbox services concurrently."""
    log_level = os.environ.get("CLOUDBOX_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s  %(name)-22s  %(levelname)-8s  %(message)s",
    )

    logger.info("Starting Cloudbox  (log level: %s)", log_level)
    logger.info("")
    logger.info(
        "  %-20s → grpc://%s:%d  (PUBSUB_EMULATOR_HOST=%s:%d)",
        "Cloud Pub/Sub gRPC",
        settings.host,
        settings.pubsub_port,
        settings.host,
        settings.pubsub_port,
    )
    for _name, label, port_fn in _SERVICES:
        logger.info("  %-20s → http://%s:%d", label, settings.host, port_fn())
    logger.info("")
    logger.info("SDK usage:")
    logger.info(
        "  Pub/Sub (default gRPC):  set PUBSUB_EMULATOR_HOST=localhost:%d", settings.pubsub_port
    )
    logger.info(
        "  Pub/Sub (REST):          transport='rest', api_endpoint='http://localhost:%d'",
        settings.pubsub_rest_port,
    )
    logger.info("  Firestore:               use transport='rest' in ClientOptions")
    logger.info("  Other services:          REST is the default — no extra config needed")
    logger.info("")

    configs = _build_configs()
    asyncio.run(_serve_all(configs))


if __name__ == "__main__":
    main()
