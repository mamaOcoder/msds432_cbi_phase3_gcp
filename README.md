# 3.GCP-Docker-PG-Go
This folder contains the code for the Google Cloud Platform implementation stage. In this stage, the code written in the localhost stage is pushed and hosted on GCP Cloud Run. To run this, the user must have GCP account and project.

Note that we are setting up the PostgreSQL database directly on GCP, so the code in our Go application to create the database should just return that the database already exists. The main function was also updated to include http handler information. I was unable to get the postgis extension added- when adding it via CLI, it said that it worked, however, I get errors when my Go app actually runs. For now, I am removing the geography fields.

## Setup/Run
### Create GCP Project and PostgreSQL database
1. Install the Google Cloud CLI on your local machine.
2. Create a new project on your Google Cloud console.
3. From the command/terminal window, execute the command:  
    *gcloud init*
4. Create database instance of PostgreSQL on GCP. Execute the command:  
    *gcloud sql instances create cbipostgres --database-version=POSTGRES_14 --cpu=2 --memory=7680MB --region=us-central*
5. Create SQL users on the database instance. Execute the command:  
    *gcloud sql users set-password postgres --instance=cbipostgres --password=root*
6. Create chicago_business_intelligence database. Execute the command:  
    *gcloud sql databases create chicago_business_intelligence --instance=cbipostgres*
7. *Note: this approach did not work. For now, just removing postgis dependencies.* Add the postgis extension. Execute the commands:  
    *gcloud sql connect cbipostgres --user=postgres --quiet*  
    *CREATE EXTENSION IF NOT EXISTS postgis;*
7. Add the PostgreSQL instance connection name into Go code for host name.
8. Update cloudbuild.yaml file with project name and API Key.

### Continuous Deployment
1. Create GitHub repository for CBI source code.
2. Enable Cloud Build API for project.
3. Create a trigger and connect GitHub repository.

### Go-microservice and Pg-admin
1. Enable Cloud Run API for project.
2. Enable IAM permissions. *Note this will likely take a lot of additional work and trouble-shooting*

## View Results
1. Push source code to GitHub repo. This will trigger a new build to run.
2. Go to Cloud Run and verify services are up and running.
3. Click on each service to view log and get URL links.
