# BIKE_DE
Pipline for bike sharing data



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
