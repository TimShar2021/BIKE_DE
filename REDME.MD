Background: 

In UK there is a state bike sharing program, which exists to stimulate citizens to use bikes instead of car vehicles. 
Project problem: 

The data about the bike rides is collected with mistakes, NaNs, without clear structure.
Project Goal: To process and clear the data of all bike rides from 2012 to 2021 from Cycling.data.tfl.gov.uk and create an easy-to-read dashboard.

Project steps:

• The automatization of the data uploading to the local storage with Selenium library. At this stage the data was also cleaned from the NaNs and converted data format from csv to csv.gzip
• The transfer of the data from the local storage to the Google Cloud storage with Spark tool
• The transfer of the data from the Google Cloud storage to the Big Query with Dataproc tool
• The creation of the dashboard at Google LookerStudio 

![Bicycle rental](/bike_rental.png)


To run and evaluate the project, please complete the next steps:
• To create / log in the Big Query account according to the instruction: 

[GCP](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_1_basics_n_setup/1_terraform_gcp/2_gcp_overview.md)

• To copy the code from the GitHub: <https://github.com/TimShar2021/BIKE_DE>

• To configure GCP using the Terraform (from the Terraform folder) according to the instruction:

[Terraform](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp/terraform)

• To install the Prefect according to the instruction: 

[Prefect](https://github.com/discdiver/prefect-zoomcamp)

• To fill the tab Variables with the Variables 
• To create a Dataproc cluster in GCP. For your convenience, please use the cluster’s name ‘dezoomcluster’
• To complete the deployment of the flow in Prefect according to the instruction: 

    prefect deployment build ./main_flow.py:main_flow -n "MAIN ETL"

    prefect deployment main_flow-deployment.yaml

    • To run the Main ETL from the deployment
Then two tables will appear at the Big Query. Based on these tables the dashboard at the Lookerstudio was created. The dashboard is here: 

    [Bicycle rental in London](https://lookerstudio.google.com/s/mE1UUVb8oP4)
