## Step 1 — Data Setup

1. Go to [https://www.kaggle.com/datasets/hhs/health-insurance-marketplace/data](https://www.kaggle.com/datasets/hhs/health-insurance-marketplace/data)

2. Download the following CSVs into a /usr/Downloads/458project/Data folder

   1. BenefitsCostSharing.csv

   2. BusinessRules.csv

   3. Rate.csv

   4. ServiceArea.csv

   5. Network.csv

3. For each of the CSVs, run the following to move to docker

docker mkdir cloudera:/usr/458project

docker cp '/Users/masonholcombe/Downloads/458project/Data/' cloudera:/usr/458project/data

1. Once these files are in docker, run the following to move to HDFS

hdfs dfs -mkdir -p /user/458project/data/$filename

hdfs dfs -put /usr/458project/data/$file_name.csv /user/458project/data/$file_name/$file_name.csv

## Step 2 — HIVE

Now that the data is in HDFS, create the tables necessary to query on HIVE.

1. Access HUE through beeline, run the following:

beeline

!connect jdbc:hive2://

* username: cloudera

* password: cloudera

1. Create a database to store all of the tables and use as default:

CREATE DATABASE IF NOT EXISTS health_insurance;

USE health_insurance;

1. Run the following for each CSV to create the table in HIVE that reads from HDFS:

#### BENEFITS COST SHARING

`CREATE EXTERNAL TABLE health_insurance.benefits_cost_sharing (`

`    BenefitName STRING,`

`    BusinessYear INT,`

`    CoinsInnTier1 STRING,`

`    CoinsInnTier2 STRING,`

`    CoinsOutofNet STRING,`

`    CopayInnTier1 STRING,`

`    CopayInnTier2 STRING,`

`    CopayOutofNet STRING,`

`    EHBVarReason STRING,`

`    Exclusions STRING,`

`    Explanation STRING,`

`    ImportDate STRING,`

`    IsCovered STRING,`

`    IsEHB STRING,`

`    IsExclFromInnMOOP STRING,`

`    IsExclFromOonMOOP STRING,`

`    IsStateMandate STRING,`

`    IsSubjToDedTier1 STRING,`

`    IsSubjToDedTier2 STRING,`

`    IssuerId INT,`

`    IssuerId2 INT,`

`    LimitQty DOUBLE,`

`    LimitUnit STRING,`

`    MinimumStay DOUBLE,`

`    PlanId STRING,`

`    QuantLimitOnSvc STRING,`

`    RowNumber INT,`

`    SourceName STRING,`

`    StandardComponentId STRING,`

`    StateCode STRING,`

`    StateCode2 STRING,`

`    VersionNum DOUBLE`

`)`

`ROW FORMAT DELIMITED`

`FIELDS TERMINATED BY ','`

`STORED AS TEXTFILE`

`LOCATION '/user/458project/data/BenefitsCostSharing'`

`TBLPROPERTIES ("skip.header.line.count"="1");`

#### BUSINESS RULES

`CREATE EXTERNAL TABLE health_insurance.business_rules (`

`    BusinessYear INT,`

`    StateCode STRING,`

`    IssuerId INT,`

`    IssuerId2 INT,`

`    SourceName STRING,`

`    VersionNum DOUBLE,`

`    ImportDate STRING,`

`    TIN STRING,`

`    ProductId STRING,`

`    StandardComponentId STRING,`

`    EnrolleeContractRateDeterminationRule STRING,`

`    TwoParentFamilyMaxDependentsRule STRING,`

`    SingleParentFamilyMaxDependentsRule STRING,`

`    DependentMaximumAgRule STRING,`

`    ChildrenOnlyContractMaxChildrenRule STRING,`

`    DomesticPartnerAsSpouseIndicator STRING,`

`    SameSexPartnerAsSpouseIndicator STRING,`

`    AgeDeterminationRule STRING,`

`    MinimumTobaccoFreeMonthsRule STRING,`

`    CohabitationRule STRING,`

`    RowNumber INT,`

`    MarketCoverage STRING,`

`    DentalOnlyPlan STRING`

`)`

`ROW FORMAT DELIMITED`

`FIELDS TERMINATED BY ','`

`STORED AS TEXTFILE`

`LOCATION '/user/458project/data/BusinessRules'`

`TBLPROPERTIES ("skip.header.line.count"="1");`

#### NETWORK

`CREATE EXTERNAL TABLE health_insurance.network (`

`    BusinessYear INT,`

`    StateCode STRING,`

`    IssuerId INT,`

`    SourceName STRING,`

`    VersionNum DOUBLE,`

`    ImportDate STRING,`

`    IssuerId2 INT,`

`    StateCode2 STRING,`

`    NetworkName STRING,`

`    NetworkId STRING,`

`    NetworkURL STRING,`

`    RowNumber INT,`

`    MarketCoverage STRING,`

`    DentalOnlyPlan STRING`

`)`

`ROW FORMAT DELIMITED`

`FIELDS TERMINATED BY ','`

`STORED AS TEXTFILE`

`LOCATION '/user/458project/data/Network'`

`TBLPROPERTIES ("skip.header.line.count"="1");`

#### RATE

`CREATE TABLE health_insurance.rate (`

`  BusinessYear INT,`

`  StateCode STRING,`

`  IssuerId INT,`

`  SourceName STRING,`

`  VersionNum INT,`

`  ImportDate STRING,`

`  IssuerId2 INT,`

`  FederalTIN STRING,`

`  RateEffectiveDate STRING,`

`  RateExpirationDate STRING,`

`  PlanId STRING,`

`  RatingAreaId STRING,`

`  Tobacco STRING,`

`  Age STRING,`

`  IndividualRate DOUBLE,`

`  IndividualTobaccoRate DOUBLE,`

`  Couple DOUBLE,`

`  PrimarySubscriberAndOneDependent DOUBLE,`

`  PrimarySubscriberAndTwoDependents DOUBLE,`

`  PrimarySubscriberAndThreeOrMoreDependents DOUBLE,`

`  CoupleAndOneDependent DOUBLE,`

`  CoupleAndTwoDependents DOUBLE,`

`  CoupleAndThreeOrMoreDependents DOUBLE,`

`  RowNumber INT`

`)`

`ROW FORMAT DELIMITED`

`FIELDS TERMINATED BY ','`

`STORED AS TEXTFILE`

`LOCATION '/user/458project/data/Rate'`

`TBLPROPERTIES ("skip.header.line.count"="1");`

#### SERVICE AREA

`CREATE TABLE health_insurance.service_area (`

`  BusinessYear INT,`

`  StateCode STRING,`

`  IssuerId INT,`

`  SourceName STRING,`

`  VersionNum INT,`

`  ImportDate STRING,`

`  IssuerId2 INT,`

`  StateCode2 STRING,`

`  ServiceAreaId STRING,`

`  ServiceAreaName STRING`

`)`

`ROW FORMAT DELIMITED`

`FIELDS TERMINATED BY ','`

`STORED AS TEXTFILE`

`LOCATION '/user/458project/data/ServiceArea/'`

`TBLPROPERTIES ("skip.header.line.count"="1");`

## Step 3 — Analysis

Run queries on this data from the HUE dashboard.