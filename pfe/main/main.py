import importlib

from pfe.context.context import args, job, logger

job.init(args["JOB_NAME"], args)

entrypoint = args["ENTRYPOINT"]
logger.info(f"Welcome in entrypoint={entrypoint} !")
module_name = f"pfe.process.{entrypoint}"
entrypoint_module = importlib.import_module(module_name)
entrypoint_module.run_job()

job.commit()
