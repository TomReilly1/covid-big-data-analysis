-------------------------------------------------------------------------------
------------------------------- DROP EVERYTHING -------------------------------
-------------------------------------------------------------------------------
-- Each section has its own drop statements, however, the following statements
-- can be used if a fresh restart from the beginning is preferred. Otherwise,
-- start at 'Step 1: Raw Data'.

-- RAW DATA
--DROP TABLE covid_raw_1 CASCADE CONSTRAINTS;

-- NORMALIZED DATA
DROP SEQUENCE dates_seq;
DROP SEQUENCE county_seq;
DROP SEQUENCE cases_and_deaths_seq;
DROP TABLE states CASCADE CONSTRAINTS;
DROP TABLE dates CASCADE CONSTRAINTS;
DROP TABLE counties CASCADE CONSTRAINTS;
DROP TABLE cases_and_deaths CASCADE CONSTRAINTS;

-- DIMENTIONS
DROP SEQUENCE dim_states_seq;
DROP SEQUENCE dim_months_seq;
DROP SEQUENCE dim_quarters_seq;
DROP SEQUENCE dim_counties_seq;
DROP TABLE dim_states CASCADE CONSTRAINTS;
DROP TABLE dim_dates CASCADE CONSTRAINTS;
DROP TABLE dim_months CASCADE CONSTRAINTS;
DROP TABLE dim_quarters CASCADE CONSTRAINTS;
DROP TABLE dim_counties CASCADE CONSTRAINTS;

-- FACTS
DROP SEQUENCE fact_cases_seq;
DROP SEQUENCE fact_deaths_seq;
DROP TABLE fact_cases_per_month CASCADE CONSTRAINTS;
DROP TABLE fact_deaths_per_quarter CASCADE CONSTRAINTS;



-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
------------------------------- Step 1: Raw Data ------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
---- create the table for the raw covid data
drop table covid_raw_1 CASCADE CONSTRAINTS;
create table covid_raw_1
(
    L_DATE      DATE, 
    COUNTY      varchar2(50),
    STATE       VARCHAR2(35),
    FIPS        NUMBER(10,0),
    CASES       NUMBER(10,0),
    DEATHS      NUMBER(10,0)
);


SELECT state FROM covid_raw_1;



-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
---------------------------- Step 2: Normalize Data ---------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- create states 3NF table
drop table states;
create table states (
    state_cd        VARCHAR2(50)    NOT NULL,
    state_name      VARCHAR2(50)    NOT NULL, 
    PRIMARY KEY (state_cd)
);


-- insert data into states 3NF table
INSERT INTO states VALUES ('AL', 'Alabama');
INSERT INTO states VALUES ('AK', 'Alaska');
INSERT INTO states VALUES ('AZ', 'Arizona');
INSERT INTO states VALUES ('AR', 'Arkansas');
INSERT INTO states VALUES ('CA', 'California');
INSERT INTO states VALUES ('CO', 'Colorado');
INSERT INTO states VALUES ('CT', 'Connecticut');
INSERT INTO states VALUES ('DE', 'Delaware');
INSERT INTO states VALUES ('DC', 'District of Columbia');
INSERT INTO states VALUES ('FL', 'Florida');
INSERT INTO states VALUES ('GA', 'Georgia');
INSERT INTO states VALUES ('HI', 'Hawaii');
INSERT INTO states VALUES ('ID', 'Idaho');
INSERT INTO states VALUES ('IL', 'Illinois');
INSERT INTO states VALUES ('IN', 'Indiana');
INSERT INTO states VALUES ('IA', 'Iowa');
INSERT INTO states VALUES ('KS', 'Kansas');
INSERT INTO states VALUES ('KY', 'Kentucky');
INSERT INTO states VALUES ('LA', 'Louisiana');
INSERT INTO states VALUES ('ME', 'Maine');
INSERT INTO states VALUES ('MD', 'Maryland');
INSERT INTO states VALUES ('MA', 'Massachusetts');
INSERT INTO states VALUES ('MI', 'Michigan');
INSERT INTO states VALUES ('MN', 'Minnesota');
INSERT INTO states VALUES ('MS', 'Mississippi');
INSERT INTO states VALUES ('MO', 'Missouri');
INSERT INTO states VALUES ('MT', 'Montana');
INSERT INTO states VALUES ('NE', 'Nebraska');
INSERT INTO states VALUES ('NV', 'Nevada');
INSERT INTO states VALUES ('NH', 'New Hampshire');
INSERT INTO states VALUES ('NJ', 'New Jersey');
INSERT INTO states VALUES ('NM', 'New Mexico');
INSERT INTO states VALUES ('NY', 'New York');
INSERT INTO states VALUES ('NC', 'North Carolina');
INSERT INTO states VALUES ('ND', 'North Dakota');
INSERT INTO states VALUES ('OH', 'Ohio');
INSERT INTO states VALUES ('OK', 'Oklahoma');
INSERT INTO states VALUES ('OR', 'Oregon');
INSERT INTO states VALUES ('PA', 'Pennsylvania');
INSERT INTO states VALUES ('RI', 'Rhode Island');
INSERT INTO states VALUES ('SC', 'South Carolina');
INSERT INTO states VALUES ('SD', 'South Dakota');
INSERT INTO states VALUES ('TN', 'Tennessee');
INSERT INTO states VALUES ('TX', 'Texas');
INSERT INTO states VALUES ('UT', 'Utah');
INSERT INTO states VALUES ('VT', 'Vermont');
INSERT INTO states VALUES ('VA', 'Virginia');
INSERT INTO states VALUES ('WA', 'Washington');
INSERT INTO states VALUES ('WV', 'West Virginia');
INSERT INTO states VALUES ('WI', 'Wisconsin');
INSERT INTO states VALUES ('WY', 'Wyoming');


SELECT * FROM states;


-------------------------------------------------------------------------------
-- create sequence for dates
drop sequence dates_seq;
CREATE SEQUENCE dates_seq
	INCREMENT BY 1
	START WITH   1
	MAXVALUE     9999999
	MINVALUE     1
	CYCLE;


-- create dates 3NF table
drop table dates;
create table dates
(
    date_id         NUMBER          NOT NULL,
    l_date          DATE            NOT NULL,
    PRIMARY KEY (date_id)
);


-- insert raw data into dates 3NF table
insert into dates (date_id, l_date)
select dates_seq.nextval, l_date
FROM (select distinct l_date
    from covid_raw_1
    WHERE fips is NOT NULL
    AND fips < 65000
    ORDER BY l_date)
;


SELECT * FROM dates ORDER BY date_id;


-------------------------------------------------------------------------------
-- create sequence for covid counties
drop sequence county_seq ;
CREATE SEQUENCE county_seq
	INCREMENT BY 1
	START WITH   1
	MAXVALUE     9999999
	MINVALUE     1
	CYCLE;


-- create counties 3NF table
drop table counties;
create table counties
(
    fips            NUMBER          NOT NULL,
    county_name     VARCHAR2(50)    NOT NULL, 
    PRIMARY KEY (fips)
);


-- insert raw data into counties 3NF table
INSERT INTO counties
SELECT DISTINCT
    cr.fips,
    cr.county
FROM covid_raw_1 cr
WHERE fips is NOT NULL  -- excludes nulls
AND county IS NOT NULL
AND fips < 65000    -- excludes non-states (e.g. Puerto Rico, Virgin Islands, etc.)
;


SELECT * FROM counties ORDER BY fips;


-------------------------------------------------------------------------------
-- create sequence for cases_and_deaths
drop sequence cases_and_deaths_seq;
CREATE SEQUENCE cases_and_deaths_seq
	INCREMENT BY 1
	START WITH   1
	MAXVALUE     9999999
	MINVALUE     1
	CYCLE;


-- create cases_and_deaths 3NF table
DROP TABLE cases_and_deaths;
CREATE TABLE cases_and_deaths
(
    cad_id NUMBER PRIMARY KEY,
    cad_date DATE,
    cad_fips NUMBER REFERENCES counties(fips),
    cases_count NUMBER,
    deaths_count NUMBER
);


-- insert raw data into cases_and_deaths 3NF table
INSERT INTO cases_and_deaths
SELECT  cases_and_deaths_seq.nextval,
        cr.l_date,
        c.fips,
        cr.cases,
        cr.deaths
FROM covid_raw_1 cr, counties c
WHERE c.fips = cr.fips
AND cr.fips IS NOT NULL  -- excludes nulls
AND cr.county IS NOT NULL
AND cr.fips < 65000  -- excludes non-states (e.g. Puerto Rico, Virgin Islands, etc.)
;


SELECT * FROM cases_and_deaths ORDER BY cad_fips, cad_date;



-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--------------------------- Step 3: Dimension Tables---------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- create sequence for states dimension table
drop sequence dim_states_seq;
CREATE SEQUENCE dim_states_seq
	INCREMENT BY 1
	START WITH   1
	MAXVALUE     99
	MINVALUE     1
	CYCLE;


-- create states dimension table
drop table dim_states;
create table dim_states (
    state_id        NUMBER          NOT NULL,
    state_cd        VARCHAR2(50)    NOT NULL,
    state_name      VARCHAR2(50)    NOT NULL, 
    PRIMARY KEY (state_id)
);


-- insert date into states dimension table using data from states 3NF table
INSERT INTO dim_states SELECT
    dim_states_seq.nextval,
    state_cd,
    state_name
FROM states;


SELECT * FROM dim_states; 


-------------------------------------------------------------------------------
-- create dates dimension table and insert days from 04/01/2020 to 03/31/2022
drop table dim_dates;
CREATE TABLE dim_dates AS
SELECT n AS date_id,
    TO_DATE('03/31/2020', 'MM/DD/YYYY') + NUMTODSINTERVAL(n, 'day') AS full_date,
    TO_CHAR(TO_DATE('03/31/2020', 'MM/DD/YYYY') + NUMTODSINTERVAL(n, 'day'), 'Mon') as month_short,
    TO_CHAR(TO_DATE('03/31/2020', 'MM/DD/YYYY') + NUMTODSINTERVAL(n, 'day'), 'Month') as month_long,
    TO_CHAR(TO_DATE('03/31/2020', 'MM/DD/YYYY') + NUMTODSINTERVAL(n, 'day'), 'YYYY') as year_long
FROM 
(
    SELECT LEVEL n
    FROM dual
    CONNECT BY LEVEL <= 730  -- days in year(365) * number of years(2) = 730
);


-- add quarter column
ALTER TABLE dim_dates
ADD quarter_long VARCHAR2(50);


select * from dim_dates;


-- update quarters
UPDATE dim_dates SET quarter_long = 'quarter_1'
WHERE
    month_long LIKE 'Jan%'
    OR month_long LIKE 'Feb%'
    OR month_long LIKE 'Mar%'
;
UPDATE dim_dates SET quarter_long = 'quarter_2'
WHERE
    month_long LIKE 'Apr%'
    OR month_long LIKE 'May%'
    OR month_long LIKE 'Jun%'
;
UPDATE dim_dates SET quarter_long = 'quarter_3'
WHERE
    month_long LIKE 'Jul%'
    OR month_long LIKE 'Aug%'
    OR month_long LIKE 'Sep%'
;
UPDATE dim_dates SET quarter_long = 'quarter_4'
WHERE
    month_long LIKE 'Oct%'
    OR month_long LIKE 'Nov%'
    OR month_long LIKE 'Dec%'
;


select * from dim_dates;


-------------------------------------------------------------------------------
-- create months dimension sequence
drop sequence dim_months_seq;
CREATE SEQUENCE dim_months_seq
	INCREMENT BY 1
	START WITH   1
	MAXVALUE     999999
	MINVALUE     1
	CYCLE;


-- create months dimension table from April 2020 to March 2022
DROP TABLE dim_months;
CREATE TABLE dim_months
(
    dim_months_id NUMBER PRIMARY KEY,
    month_name VARCHAR(50),
    year_name NUMBER,
    last_day DATE
);


INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'April', '2020', TO_DATE('04/30/2020', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'May', '2020', TO_DATE('05/31/2020', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'June', '2020', TO_DATE('06/30/2020', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'July', '2020', TO_DATE('07/31/2020', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'August', '2020', TO_DATE('08/31/2020', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'September', '2020', TO_DATE('09/30/2020', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'October', '2020', TO_DATE('10/31/2020', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'November', '2020', TO_DATE('11/30/2020', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'December', '2020', TO_DATE('12/31/2020', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'January', '2021', TO_DATE('01/31/2021', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'February', '2021', TO_DATE('02/28/2021', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'March', '2021', TO_DATE('03/31/2021', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'April', '2021', TO_DATE('04/30/2021', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'May', '2021', TO_DATE('05/31/2021', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'June', '2021', TO_DATE('06/30/2021', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'July', '2021', TO_DATE('07/31/2021', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'August', '2021', TO_DATE('08/31/2021', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'September', '2021', TO_DATE('09/30/2021', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'October', '2021', TO_DATE('10/31/2021', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'November', '2021', TO_DATE('11/30/2021', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'December', '2021', TO_DATE('12/31/2021', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'January', '2022', TO_DATE('01/31/2022', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'February', '2022', TO_DATE('02/28/2022', 'MM/DD/YYYY'));
INSERT INTO dim_months VALUES (dim_months_seq.nextval, 'March', '2022', TO_DATE('03/31/2022', 'MM/DD/YYYY'));


SELECT * FROM dim_months;


-------------------------------------------------------------------------------
-- create quarters dimension sequence
drop sequence dim_quarters_seq;
CREATE SEQUENCE dim_quarters_seq
	INCREMENT BY 1
	START WITH   1
	MAXVALUE     999999
	MINVALUE     1
	CYCLE;


-- create quarters dimension table
DROP TABLE dim_quarters;
CREATE TABLE dim_quarters
(
    dim_quarter_id NUMBER PRIMARY KEY,
    quarter_name VARCHAR(50),
    year_name NUMBER,
    last_day DATE
);

INSERT INTO dim_quarters VALUES (dim_quarters_seq.nextval, 'Quarter 2', '2020', TO_DATE('06/30/2020', 'MM/DD/YYYY'));
INSERT INTO dim_quarters VALUES (dim_quarters_seq.nextval, 'Quarter 3', '2020', TO_DATE('09/30/2020', 'MM/DD/YYYY'));
INSERT INTO dim_quarters VALUES (dim_quarters_seq.nextval, 'Quarter 4', '2020', TO_DATE('12/31/2020', 'MM/DD/YYYY'));
INSERT INTO dim_quarters VALUES (dim_quarters_seq.nextval, 'Quarter 1', '2021', TO_DATE('03/31/2021', 'MM/DD/YYYY'));
INSERT INTO dim_quarters VALUES (dim_quarters_seq.nextval, 'Quarter 2', '2021', TO_DATE('06/30/2021', 'MM/DD/YYYY'));
INSERT INTO dim_quarters VALUES (dim_quarters_seq.nextval, 'Quarter 3', '2021', TO_DATE('09/30/2021', 'MM/DD/YYYY'));
INSERT INTO dim_quarters VALUES (dim_quarters_seq.nextval, 'Quarter 4', '2021', TO_DATE('12/31/2021', 'MM/DD/YYYY'));
INSERT INTO dim_quarters VALUES (dim_quarters_seq.nextval, 'Quarter 1', '2022', TO_DATE('03/31/2022', 'MM/DD/YYYY'));


SELECT * FROM dim_quarters;


-------------------------------------------------------------------------------
-- create sequence for counties dimension table
drop sequence dim_counties_seq;
CREATE SEQUENCE dim_counties_seq
	INCREMENT BY 1
	START WITH   1
	MAXVALUE     9999999
	MINVALUE     1
	CYCLE;


-- create states dimension table
drop table dim_counties;
create table dim_counties (
    dim_county_id       NUMBER,
    fips                NUMBER,
    county_nm           VARCHAR(50),
    PRIMARY KEY (dim_county_id)
);


INSERT INTO dim_counties
SELECT
    dim_counties_seq.nextval,
    c.fips,
    c.county_name
FROM counties c;


SELECT * FROM dim_counties ORDER BY fips;



-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
---------------------------- Step 4: Facts Tables -----------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- create sequence for cases per month fact table
drop sequence fact_cases_seq;
CREATE SEQUENCE fact_cases_seq
	INCREMENT BY 1
	START WITH   1
	MAXVALUE     99999999
	MINVALUE     1
	CYCLE;


-- create fact table for cases per month
drop table fact_cases_per_month;
create table fact_cases_per_month (
    fact_cpm_key    NUMBER PRIMARY KEY,
    county_id       NUMBER REFERENCES dim_counties(dim_county_id),
    month_id        NUMBER REFERENCES dim_months(dim_months_id),
    cases_count     NUMBER
);


INSERT INTO fact_cases_per_month
SELECT
    fact_cases_seq.nextval,
    dc.dim_county_id,
    dm.dim_months_id,
    cad.cases_count
FROM dim_counties dc, dim_months dm, cases_and_deaths cad
WHERE dm.last_day = cad.cad_date
AND dc.fips = cad.cad_fips
;


SELECT * FROM fact_cases_per_month;


-------------------------------------------------------------------------------
-- create sequence for deaths per quarter fact table
drop sequence fact_deaths_seq;
CREATE SEQUENCE fact_deaths_seq
	INCREMENT BY 1
	START WITH   1
	MAXVALUE     99999999
	MINVALUE     1
	CYCLE;


-- create fact table for deaths per quarter
drop table fact_deaths_per_quarter;
create table fact_deaths_per_quarter (
    fact_cpm_key    NUMBER PRIMARY KEY,
    county_id       NUMBER REFERENCES dim_counties(dim_county_id),
    quarter_id      NUMBER REFERENCES dim_quarters(dim_quarter_id),
    deaths_count    NUMBER
);


INSERT INTO fact_deaths_per_quarter
SELECT
    fact_deaths_seq.nextval,
    dc.dim_county_id,
    dq.dim_quarter_id,
    cad.deaths_count
FROM dim_counties dc, dim_quarters dq, cases_and_deaths cad
WHERE dq.last_day = cad.cad_date
AND dc.fips = cad.cad_fips
;


SELECT * FROM fact_deaths_per_quarter;



-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
------------------------------- Step 5: Reports -------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Report 1: Total Cases (accumulated) in U.S., measured by month
SELECT sum(cpm.cases_count), cpm.month_id, dm.month_name, dm.year_name
FROM fact_cases_per_month cpm, dim_months dm
WHERE cpm.month_id = dm.dim_months_id
GROUP BY cpm.month_id, dm.month_name, dm.year_name
ORDER BY cpm.month_id;


-- Report 2: Total Deaths (accumulated) in U.S., measured by quarter
SELECT sum(dpq.deaths_count), dpq.quarter_id, dq.quarter_name, dq.year_name
FROM fact_deaths_per_quarter dpq, dim_quarters dq
WHERE dpq.quarter_id = dq.dim_quarter_id
GROUP BY dpq.quarter_id, dq.quarter_name, dpq.quarter_id, dq.year_name
ORDER BY dpq.quarter_id;


