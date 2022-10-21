# Transfering Files from CDE Resources to ADLS

## Objective

CDE is the Cloudera Data Engineering Service, a containerized managed service for Large Scale Batch Pipelines with Spark featuring Airflow and Iceberg.
A CDE Resource in Cloudera Data Engineering (CDE) is a named collection of files used by a job. They can include application code, configuration files, custom Docker images (Private Cloud only), and Python virtual environment specifications.

Overall, CDE Resources simplify the management of files and dependencies associated with complex pipelines. Dependencies and environments are more quickly reproducible and reusable by other pipelines.

This tutorial shows how to push a file from a CDE Resource to ADLS. Although CDE provides a fully managed Airflow instance with each Virtual Cluster, Airflow orchestration is optional. The pattern shown in this tutorial provides an example integration between CDE and 3rd party orchestration systems.

![alt text](img/step_title.png)

## Requirements

The following requirements are needed to run through the tutorial:

* CDP Public Cloud: A CDE Virtual Cluster associated with an Azure CDP Instance.
* Virtual Clusters of Spark 2 or Spark 3 version are ok.
* Azure: The Storage Account Name and the Storage Account Key associated with the ADLS target destination for the file transfer
* Basic familiarity with Python (no code changes are required)

## Instructions

#### Step 0: Project setup

##### Clone the Project

Clone this GitHub repository to your local computer.

```
mkdir ~/Documents/CDEResource2ADLS_Tutorial
cd ~/Documents/CDEResource2ADLS_Tutorial
git clone https://github.com/pdefusco/CDEResource2ADLS.git
```

Alternatively, if you don't have GitHub create a folder on your local computer; navigate to [this URL](https://github.com/pdefusco/CDEResource2ADLS.git) and download the files.

![alt text](img/step0.png)

##### Add your ADLS Storage Info to cde_job.py

Open "cde_job.py" in any editor of your choice. Add your Azure Storage Account Name and Key in the fields at lines 89 and 90.
Please reach out to your Azure admin if you are unsure what these are.

#### Step 1: Create a CDE Resource of Type Files

In this step you will upload our Python scripts to a CDE Resource of type "File" so we can more easily create a CDE Job with its dependencies.

Navigate to your CDE Virtual Cluster and Open the Jobs page.

![alt text](img/step1.png)

From the left tab, select Resources and create a CDE Resource of type "File".

![alt text](img/step2.png)

![alt text](img/step3.png)

Next, upload the "cde_job.py", "my_file1.py" and "utils.py" files via the "Upload" icon.

![alt text](img/step4.png)

#### Step 2: Create a CDE Resource of Type Python Environment

In this step you will create a Python Environment so you can pip install the "azure-storage-file-datalake" Library and use it with your CDE Job.

Navigate back to the CDE Resources tab and create a CDE Resource of type "Python Environment". Select Python 3 and leave the PiPy mirror field blank.

![alt text](img/step5.png)

Upload "requirements.txt" from your local machine and allow a few seconds for the Python environment to build.

![alt text](img/step6A.png)

When the build is complete, exit to the Resources page and validate that you now see two entries. One of type "Files" and one of type "Python".

![alt text](img/step6.png)

#### Step 3: Create a CDE Job

In this step you will create the CDE Job with the uploaded scripts and Python environment.

Navigate to the Jobs tab on the left pane. Select the "Create Job" blue icon on the right side.

![alt text](img/step6.png)

Next, select the Spark Job Type from the Toggle Bar. The tutorial is compatible with both Spark 2 or Spark 3 clusters.

![alt text](img/step7.png)

Under "Application Files" select "File" and then "Select from Resource". Select file "cde_job.py". This will be the base script for the CDE Job.

![alt text](img/step8.png)

![alt text](img/step9.png)

Immediately below, choose Python 3 and select the Python Environment you created earlier.

![alt text](img/step10.png)

![alt text](img/step11.png)

Expand the "Advanced Options" section. No changes are required but notice the CDE Resource of type file has been associated with the CDE Job for you automatically. This will allow you to reference modules in the "utils.py" script from the "cde_job.py" script.

![alt text](img/step12.png)

Finally, click on the "Create and Run" blue icon at the bottom of the page.

![alt text](img/step13.png)


#### Step 4: Validate the "my_file1.py" Upload to ADLS

The CDE Job is now executing. This will move the "my_file1.py" file from the /app/mount folder to the ADLS destination you specified in the "cde_job.py" script.

Navigate to the CDE Job Runs page and open the latest job run by clicking on its number.

![alt text](img/step14.png)

Open the "Logs" tab and then the "stdout" tab.

![alt text](img/step15.png)

At the bottom, validate that the file has been uploaded to the ADLS folder.

![alt text](img/step16.png)

Open the "cde_job.py" and "utils.py" scripts and inspect them. In summary:

* When the CDE Job is executed a Kubernetes pod running the CDE Docker Container is launched by the CDE Service. This process is fully automatic and requires no Kubernetes knowledge on the user.
* The CDE Job mounts dependencies stored in CDE Resources of type File from the /app/mount folder. Thus, all files in the CDE Resource can now be consumed by the Python script. The /app/mount folder is referenced at line 47 in "utils.py"
* The CDE Job container imports all modules from the selected Python Environment and "utils.py" file (lines 50, 51, 52, 55). The utils module methods are used between lines 99 - 127.
* The helper methods ensure the ADLS destination folder is ready for the file transfer and then upload the raaw file.
* Lines 59 - 81 show some sample Spark code. Notice this is independent of the file transfer and has only been added for demo purposes.

## Conclusions & Next Steps

CDE is the Cloudera Data Engineering Service, a containerized managed service for Spark and Airflow. With Resources, CDE users can more easily track and reuse dependencies including simple python scripts, jars, as well as Python environments and more.

In this tutorial we uploaded a file from a CDE Resource to an ADLS folder. This pattern can be as an integration point between CDE Jobs/Resources and 3rd party orchestration systems.

If you are exploring CDE you may find the following tutorials relevant:

* [Spark 3 & Iceberg](https://github.com/pdefusco/Spark3_Iceberg_CML): A quick intro of Time Travel Capabilities with Spark 3.

* [Simple Intro to the CDE CLI](https://github.com/pdefusco/CDE_CLI_Simple): A simple introduction to the CDE CLI for the CDE beginner.

* [CDE CLI Demo](https://github.com/pdefusco/CDE_CLI_demo): A more advanced CDE CLI reference with additional details for the CDE user who wants to move beyond the basics.

* [GitLab2CDE](https://github.com/pdefusco/Gitlab2CDE): a CI/CD pipeline to orchestrate Cross-Cluster Workflows for Hybrid/Multicloud Data Engineering.

* [CML2CDE](https://github.com/pdefusco/cml2cde_api_example): an API to create and orchestrate CDE Jobs from any Python based environment including CML. Relevant for ML Ops or any Python Users who want to leverage the power of Spark in CDE via Python requests.

* [Postman2CDE](https://github.com/pdefusco/Postman2CDE): An example of the Postman API to bootstrap CDE Services with the CDE API.

* [Oozie2CDEAirflow API](https://github.com/pdefusco/Oozie2CDE_Migration): An API to programmatically convert Oozie workflows and dependencies into CDE Airflow and CDE Jobs. This API is designed to easily migrate from Oozie to CDE Airflow and not just Open Source Airflow.
