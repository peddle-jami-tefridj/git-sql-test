CREATE OR REPLACE VIEW ANALYTICS.TABLAB_PROTO.AGENT_FILL_RATE
    COMMENT = 'Each record represents one employee, HR details, hire start and end date, and metrics for calculating fill rate percentages.'
AS
    // Author: Mason Dutton
    // Create Date: 12/7/2021
    // Last Modified By:
    // Last Modified Date:
        // This is a quick copy of ANALYTICS.DOMO.HIRED_EMPLOYEES which populates the Fill Rate cards for SE in Domo.
        // The view is meant to be temporary until Fill Rate metrics are finalized with Service Excellence.
        // The view has been modified slightly in order to allow for calculations for Fill Rate on any date.

WITH Employee AS
    (
        SELECT DISTINCT
             "Warehouse Employee ID"
            ,"Employee Name"
            ,LOWER("Employee Email") AS "Employee Email"
            ,"Employee Company"
            ,"Employee Department"
            ,"Employee Hire Date"
            ,"Job Title Effective Begin DateTime UTC"
            ,to_date(
                CASE WHEN DATEDIFF('day',"Employee Hire Date","Job Title Effective Begin DateTime UTC") <=1
                THEN "Employee Hire Date"
                ELSE MIN("Job Title Effective Begin DateTime UTC")
                    OVER (Partition by "Employee ID","Employee Department")
                END) AS "Employee Department Hire Date"
        FROM "PROD_WAREHOUSE"."WAREHOUSE"."DIM_EMPLOYEE_VIEW"
        WHERE "Employee Data Source" = 'BambooHR'
    ),

    Humanity AS
    (
        SELECT
             schedule."Employee  Key" AS "Employee Key"
            ,timesheet."Employee Actual Shift Start Time UTC"
            ,timesheet."Employee Actual Shift End Time UTC"
        FROM "PROD_WAREHOUSE"."WAREHOUSE"."FACT_HUMANITY_TIMESHEETS_VIEW" AS timesheet
        INNER JOIN "PROD_WAREHOUSE"."WAREHOUSE"."FACT_HUMANITY_SCHEDULE_VIEW" AS schedule
            ON timesheet."Employee Shift Key" = schedule."Employee Shift Key"
    ),

    Employee_humanity AS
    (
        SELECT DISTINCT
             Employee."Employee Name"
            -- Added Employee Email so that this dataset could join into Service Excellence Performance Metrics
            ,Employee."Employee Email"
            ,Employee."Employee Company"
            ,Employee."Employee Department"
            ,Employee."Employee Hire Date"
            ,Employee."Employee Department Hire Date"
            ,IFF("Employee Department Hire Date" is not null, 1, 0)       "Planned"
            ,to_date(MIN("Employee Actual Shift Start Time UTC")
                OVER (Partition by "Employee Name","Employee Department")) "Employee First Shift Start Date"
            ,to_date(MAX("Employee Actual Shift Start Time UTC")
                OVER (Partition by "Employee Name","Employee Department")) "Employee Last Shift Start Date"
        FROM Employee
        LEFT JOIN Humanity
            ON Humanity."Employee Key" = Employee."Warehouse Employee ID"
    ),

     Employee_details AS
     (
        SELECT
            Employee_humanity.*
             -- Added Days Employed as a way of measuring dynamically certain Fill Rate Milestones.
            ,DATEDIFF('day',"Employee Department Hire Date","Employee Last Shift Start Date")                   "Days Employed"
            // The below CASE WHEN statements have been commented out because they can be answered in a more dynamic way through the Days Employed field above.
            , IFF(DATEDIFF('day', "Employee Department Hire Date", "Employee First Shift Start Date") = 0, 1, 0) "Role Filled"
--             ,CASE WHEN DATEDIFF('day',"Employee Department Hire Date","Employee Last Shift Start Date") > 5 THEN 1 ELSE 0 END "Has Completed Training"
            // Per conversations with Service Excellence ideally the Completed Training step should be 14 days after first start date.
--             ,CASE WHEN DATEDIFF('day',"Employee Department Hire Date","Employee Actual Last Start Date") > 14 THEN 1 ELSE 0 END "Has Completed Training"
--             ,CASE WHEN DATEDIFF('day',"Employee Department Hire Date","Employee Last Shift Start Date") > 30 THEN 1 ELSE 0 END "Employment Term Greater Than Thirty Days"
            ,RANK() OVER (Partition by "Employee Name" Order by "Employee Department Hire Date" desc) AS         "Rank"
            FROM Employee_humanity
    ),
    Employee_final AS
    (
        SELECT
            "Employee Name",
            "Employee Email",
            "Employee Company",
            "Employee Department",
            "Employee Department Hire Date" as "Employee Hire Date",
            "Planned",
            "Employee First Shift Start Date",
            "Employee Last Shift Start Date",
            "Days Employed",
            "Role Filled"
--             "Has Completed Training",
--             "Employment Term Greater Than Thirty Days"
        FROM Employee_Details
    )

SELECT
    *

FROM Employee_final
-- WHERE 1=1
    // Mukhtar Khaleem appears twice and has a large negative Days Employed.
--     AND "Employee Name" = 'Mukhtar Khaleem'
;


SELECT
--        "Employee Name",
--        "Employee Email",
--        "Employee Company",
--        "Employee Department",
--        "Employee Hire Date",
       DATE_TRUNC(MONTH,"Employee First Shift Start Date") START_MONTH,
       SUM("Planned") TOT_PLANNED
--        "Employee Actual Last Start Date",
--        "Days Employed",
--        "Role Filled",
--        "Has Completed Training",
--        "Employment Term Greater Than Thirty Days"
FROM ANALYTICS.TABLAB_DEV.AGENT_FILL_RATE
WHERE 1=1
    AND START_MONTH > '2020-12-31'
GROUP BY 1
ORDER BY 1 DESC
;

SELECT *
FROM ANALYTICS.TABLAB_DEV.AGENT_FILL_RATE
WHERE "Employee Name" LIKE 'Esteban Pecina'
-- ORDER BY
LIMIT 100
;




