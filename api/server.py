from fastapi import FastAPI,HTTPException
from jobrunner.parser import parse_pipeline
from jobrunner.database import create_job,create_steps
from jobrunner.engine import run_job
from jobrunner.database import list_jobs
app = FastAPI(title="JobRunner Web API")

# HOME 
@app.get("/")
def home():
    return {"message": "JobRunner Web API is running"}


# Run Jobs
@app.post("/run")
def run_pipeline(pipeline_file:str):
    '''
    Run a pipeline from YAML
    '''
    try:
        pipeline = parse_pipeline(pipeline_file)

        job_id = create_job(pipeline["name"])
        create_steps(job_id,pipeline["steps"])
    
        run_job(job_id)

        return{
            "message" : "Pipeline executed",
            "job_id" : job_id,
        }
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))
    

# Check Jobs
@app.get("/jobs")
def get_jobs():
    """
    Return all the jobs in the pipeline
    """
    jobs = list_jobs()

    results = []

    for job in jobs:
        job_id,name,status,created = job 

        results.append(
            {"job_id": job_id,
             "pipeline" : name,
             "status": status,
             "creatd_at": created}
        )

    return results

    