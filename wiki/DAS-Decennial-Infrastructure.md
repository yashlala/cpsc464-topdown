This is an overview of the implementation of the DAS TDA (Top Down
Algorithm) used to protect personal information in the 2020 Decennial
data products. More details can be found
[here](DAS-Implementation-Details).

For the 2020 Census, Disclosure Avoidance System development and
production was based in an AWS GovCloud instance managed by a
technical integrator under contract to the Census Bureau. This was
known as the TI Cloud and consisted of three environments for the DAS
and a management environment shared with other Decennial activities.

# Environments

The three environments used exclusively for DAS are:
* ITE/DEV is used for developing the software and for experimenting with different privacy configurations and strategies.
* PROD is used to generate the official privacy-protected microdata detail files used to generate the Bureau's official statistics from the 2020 Census.
* STAGE is used to test the final configuration of the TDA which was run on PROD.

The application of the DAS to a particular set of inputs with a
particular configuration is called a **mission**. Nearly all DAS
missions occur in the ITE/DEV environment with only final and official
missions occurring in PROD. In the lead up to the production run for
DHC, the DAS team executed a series of experimental missions to
balance the accuracy of summarized results with privacty protections
for individual data.

# AWS

In the AWS environment, the DAS makes direct use of EC2/EBS (compute),
S3 (storage), and EMR (high performance parallel compute). Resources
shared across runs include 2-3 Gurobi license servers and a custom
dashboard application (Apache) with an accompanying database server
(Amazon RDS). The DAS uses Amazon's SQS and SNS (communication)
services to report to the dashboard application and also uses Splunk
(provided by the TI cloud) for logging.

DAS development relies on shared DevOps services provided by the TI cloud. These include:
* a Github Enterprise server used for source code management, issues and workflow (Kanban), and configuration management
* a Jenkins server used for CI testing
* an Artifactory instance hosting software repositories of packages for Linux (yum) and Python (pypi).

The EMR clusters use AWS-provided AMIs (Amazon Machine Images) running Amazon Linux. All other AWS nodes run RedHat Enterprise Linux.

The DAS team also makes use of seven dedicated "science servers" used for experimentation and development.

# Security

The DAS uses AWS's KMS (Key Management Service) to provision security
keys which support encryption on all underlying communication. All
persistent storage (both EBS and S3) provides encryption at rest.



