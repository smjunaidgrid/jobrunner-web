import typer
import os
from jobrunner.engine import run_job
from pathlib import Path
from jobrunner.parser import parse_pipeline
from jobrunner.database import create_tables, create_job, create_steps
from jobrunner.database import get_job, get_steps
from jobrunner.database import list_jobs
from jobrunner.database import get_failed_steps, reset_failed_steps
from jobrunner.engine import run_job

app = typer.Typer(help="Jobrunner CLI")

cli = typer.Typer()
app.add_typer(cli)



@cli.command()
def init():
    """
    Initialize Jobrunner environment
    """

    base = Path(".jobrunner")
    logs = base / "logs"
    db = base / "jobs.db"

    base.mkdir(exist_ok=True)
    logs.mkdir(exist_ok=True)

    if not db.exists():
        db.touch()
        typer.echo("Database created")

    # NEW LINE
    create_tables()

    typer.echo("Jobrunner initialized")



@cli.command()
def run(pipeline_file: str):
    """
    Run a pipeline from YAML definition
    """

    pipeline = parse_pipeline(pipeline_file)

    job_id = create_job(pipeline["name"])
    create_steps(job_id, pipeline["steps"])

    typer.echo(f"Job created: {job_id}")

    # execute the job
    run_job(job_id)

@cli.command()
def status(job_id: str):
    """
    Show job status and step progress
    """

    job = get_job(job_id)

    if not job:
        typer.echo("Job not found")
        return

    typer.echo(f"\nJob ID: {job[0]}")
    typer.echo(f"Pipeline: {job[1]}")
    typer.echo(f"Status: {job[2]}")
    typer.echo(f"Created: {job[3]}")
    typer.echo(f"Completed: {job[4]}\n")

    steps = get_steps(job_id)

    typer.echo("Steps:")
    for step in steps:
        name, status, retry_count, max_retries, started, completed = step

        typer.echo(
            f"- {name} | status: {status} | retries: {retry_count}/{max_retries}"
        )

@cli.command()
def list():
    """
    List all jobs
    """

    jobs = list_jobs()

    if not jobs:
        typer.echo("No jobs found")
        return

    typer.echo("\nJobs:\n")

    for job in jobs:
        job_id, name, status, created = job

        typer.echo(
            f"{job_id} | {name} | {status} | created: {created}"
        )

@cli.command()
def logs(job_id: str, step: str = None):
    """
    Show logs for a job or a specific step
    """

    log_dir = Path(f".jobrunner/logs/{job_id}")

    if not log_dir.exists():
        typer.echo("No logs found for this job")
        return

    if step:
        files = [f for f in os.listdir(log_dir) if f.startswith(step)]

        if not files:
            typer.echo("No logs found for this step")
            return

        for file in sorted(files):
            typer.echo(f"\n--- {file} ---")
            with open(log_dir / file) as f:
                typer.echo(f.read())

    else:
        for file in sorted(os.listdir(log_dir)):
            typer.echo(f"\n--- {file} ---")
            with open(log_dir / file) as f:
                typer.echo(f.read())
    
@cli.command()
def retry(job_id: str):
    """
    Retry failed steps of a job
    """

    failed = get_failed_steps(job_id)

    if not failed:
        typer.echo("No failed steps to retry")
        return

    typer.echo("Retrying failed steps...")

    reset_failed_steps(job_id)

    run_job(job_id)