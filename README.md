# 3.GCP-Docker-PG-Go
This folder contains the code for the Google Cloud Platform implementation stage. In this stage, the code written in the localhost stage is pushed and hosted on GCP Cloud Run. To run this, the user must have GCP account and project.

Note that we are setting up the PostgreSQL database directly on GCP, so the code in our Go application to create the database can be removed.

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

### Continuous Deployment
1. Create GitHub repository for CBI source code.
2. Enable Cloud Build API for project.


## View Results
