-- {
--   "Query": "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
-- DECLARE @FromDate DATETIME= CONVERT(DATE, DATEADD(month, DATEDIFF(month, 0, DATEADD(month, -12, GETDATE())), 0));
-- DECLARE @CallSummary TABLE
-- (AgentName            VARCHAR(160),
--  AgentEmail           VARCHAR(100),
--  Date                 DATE,
--  CallRejected         INT,
--  CallReceived         INT,
--  CallAbandoned        INT,
--  AverageTalkTime      INT,
--  AverageSpeedToAnswer INT,
--  CallQueue    Varchar(20),
--  [Distributed Calls] INT
-- );
-- INSERT INTO @CallSummary
--        SELECT TRIM(OfferAgentDetails.FirstName)+' '+TRIM(OfferAgentDetails.LastName) AS AgentName,
--               OfferAgentDetails.Email AS AgentEmail,
--               CAST(SellerCalls.IncomingTime AS DATE) AS Date,
--               SUM(CASE
--                       WHEN CallStatus.Status = 'Rejected'
--                       THEN 1
--                       ELSE 0
--                   END) AS CallRejected,
--               SUM(CASE
--                       WHEN CallStatus.Status = 'Completed'
--                       THEN 1
--                       ELSE 0
--                   END) AS CallReceived,
--               SUM(CASE
--                       WHEN CallStatus.Status = 'NoAnswer'
--                       THEN 1
--                       ELSE 0
--                   END) AS CallAbandoned,
--               AVG(DATEDIFF(SECOND, ConferenceParticipants.AcceptTime, ConferenceParticipants.HangupTime)) AS TalkTime,
--               AVG(DATEDIFF(SECOND, SellerCalls.QueueTime, ConferenceParticipants.AcceptTime)) AS SpeedToAnswer,
--             Queue.Name AS CallQueue,
--             COUNT(ConferenceParticipants.[ConferenceId]) AS [Distributed Calls]
--        FROM Tbl_SellerCalls AS SellerCalls
--             INNER JOIN Tbl_CallQueue AS CallQueue ON SellerCalls.Queue = CallQueue.Id
--             LEFT OUTER JOIN Tbl_Conferences AS Conference ON Conference.CallId = SellerCalls.Id
--             LEFT OUTER JOIN [dbo].[ConferenceParticipantDetail] AS ConferenceParticipants ON ConferenceParticipants.ConferenceId = Conference.Id
--             INNER JOIN Tbl_CallStatus AS CallStatus ON CallStatus.Id = ConferenceParticipants.Status
--             INNER JOIN Tbl_OfferAgentDetails AS OfferAgentDetails ON OfferAgentDetails.OfferAgentId = ConferenceParticipants.AgentId
--             INNER JOIN OfferTeam AS OfferTeam ON OfferTeam.OfferTeamId = OfferAgentDetails.OfferTeamId
--           INNER JOIN Tbl_CallQueue AS Queue ON OfferAgentDetails.Queue = Queue.id
--        WHERE SellerCalls.IncomingTime >= @FromDate
--        GROUP BY OfferAgentDetails.FirstName,
--                 OfferAgentDetails.LastName,
--                 OfferAgentDetails.Email,
--                 CAST(SellerCalls.IncomingTime AS DATE),
--              Queue.Name
-- SELECT  OfferAgentStatusData.AgentName,
--        OfferAgentStatusData.AgentEmail,
--        OfferAgentStatusData.[Date],
--        OfferAgentStatusData.OfferTeam,
--        ISNULL(CallReceived, 0) AS CallReceived,
--        ISNULL(CallAbandoned, 0) AS CallAbandoned,
--        ISNULL(CallRejected, 0) AS CallRejected,
--        ISNULL(AverageTalkTime, 0) AS AverageTalkTime,
--        ISNULL(AverageSpeedToAnswer, 0) AS AverageSpeedToAnswer,
--       ISNULL([Distributed Calls],0) AS [Distributed Calls],
--        CallQueue,
--        OfferAgentStatusData.Available,
--        OfferAgentStatusData.Busy,
--        OfferAgentStatusData.OnBreak,
--        OfferAgentStatusData.Hold,
--        OfferAgentStatusData.[Wrap-Up]
-- FROM
-- (
--     SELECT pvt.AgentName,
--            pvt.AgentEmail,
--            pvt.OfferTeam,
--            pvt.Date,
--            SUM(ISNULL(Available, 0)) AS Available,
--            SUM(ISNULL(Busy, 0)) AS Busy,
--            SUM(ISNULL(OnBreak, 0)) AS OnBreak,
--            SUM(ISNULL(Hold, 0)) AS Hold,
--            SUM(ISNULL([Wrap-Up], 0)) AS [Wrap-Up]
--     FROM
--     (
--         SELECT TRIM(OfferAgentDetails.FirstName)+' '+TRIM(OfferAgentDetails.LastName) AS AgentName,
--                OfferAgentDetails.Email AS AgentEmail,
--                MIN(CASE
--                        WHEN(OfferAgentStatus.Status = 'Login')
--                        THEN StartTime
--                    END) AS FirstLoginTime,
--                MIN(CASE
--                        WHEN(OfferAgentStatus.Status = 'Logout')
--                        THEN StartTime
--                    END) AS FirstLogoutTime,
--                SUM(DATEDIFF(s, StartTime, EndTime)) AS Duration,
--                CAST(OfferAgentStatusChange.StartTime AS DATE) AS Date,
--                OfferAgentStatus.Status AS Status,
--                ISNULL(OfferTeam.OfferTeamName, 'Undefined') AS OfferTeam
--         FROM Tbl_OfferAgentStatusChange AS OfferAgentStatusChange
--              INNER JOIN Tbl_OfferAgentStatus AS OfferAgentStatus ON OfferAgentStatusChange.Status = OfferAgentStatus.Id
--              INNER JOIN Tbl_OfferAgentDetails AS OfferAgentDetails ON OfferAgentDetails.OfferAgentId = OfferAgentStatusChange.AgentId
--              INNER JOIN OfferTeam AS OfferTeam ON OfferTeam.OfferTeamId = OfferAgentDetails.OfferTeamId
--         WHERE StartTime >= @FromDate
--         GROUP BY CAST(OfferAgentStatusChange.StartTime AS DATE),
--                  OfferAgentDetails.FirstName,
--                  OfferAgentDetails.LastName,
--                  OfferAgentDetails.Email,
--                  OfferAgentStatus.Status,
--                  OfferTeam.OfferTeamName
--     ) AS S PIVOT(SUM(Duration) FOR Status IN(Available,
--                                              Busy,
--                                              OnBreak,
--                                              Hold,
--                                              [Wrap-Up])) AS pvt
--     GROUP BY pvt.AgentName,
--              pvt.AgentEmail,
--              pvt.OfferTeam,
--              pvt.Date
-- ) AS OfferAgentStatusData
-- LEFT JOIN @CallSummary AS cs ON cs.AgentEmail = OfferAgentStatusData.AgentEmail
--                                 AND cs.Date = OfferAgentStatusData.Date

////////////////////////////////////////////////////////////////////////////////////////////////

CREATE OR REPLACE VIEW ANALYTICS.TABLAB_PROTO.TWILIO_STATUSES
    COMMENT = 'Each record is a timestamp and duration of an agent''s change in status from the Twilio system with agent details provided.'
    AS
    // Author: Mason Dutton
    // Create Date: 12/9/21

    // This is a detail table of agent status changes and timestamps.
    WITH STATUS_DETAILS AS
    (
        SELECT
              YEAR(CONVERT_TIMEZONE('America/Chicago',STARTTIME))   * 1000000 +
              MONTH(CONVERT_TIMEZONE('America/Chicago',STARTTIME))  * 10000   +
              DAY(CONVERT_TIMEZONE('America/Chicago',STARTTIME))    * 100     +
              HOUR(CONVERT_TIMEZONE('America/Chicago',STARTTIME))                   AS "STATUS_HOUR_ID"             -- This calculated ID field is how this view joins to the BPO Productivity Details datasource in Tableau.
            , CONVERT_TIMEZONE('America/Chicago',STARTTIME) :: DATE                 AS "STATUS_DATE"                -- This converted Date field is to join to Service Excellence Performance Metrics
            , oasc.AGENTID                                                          AS "AGENT_ID_FK"                -- FK for TBL_OFFERAGENTDETAILS
            , stat.STATUS                                                           AS "AGENT_STATUS"               -- The status description for the agent per the Twilio Platform.
            , CHANGEDBY                                                             AS "STATUS_CHANGE_BY"           -- Description of the source of change in status (System/Agent).
            , CONVERT_TIMEZONE('America/Chicago',STARTTIME)                         AS "STATUS_START_AT"            -- The timestamp for the start of a status.
            , CONVERT_TIMEZONE('America/Chicago',ENDTIME)                           AS "STATUS_END_AT"              -- The timestamp for the end of a status.
            , TIMEDIFF(SECONDS,STATUS_START_AT,STATUS_END_AT)                       AS "STATUS_DURATION_SECS"       -- The duration of a status in seconds.
--             , ROW_NUMBER() OVER
--                 (PARTITION BY AGENTID, STATUS_DATE ORDER BY STATUS_START_AT)        AS "TEST"
        FROM STITCH.PEDDLE.TBL_OFFERAGENTSTATUSCHANGE oasc
        LEFT JOIN STITCH.PEDDLE.TBL_OFFERAGENTSTATUS stat ON stat.ID = oasc.STATUS
    ),

    // Each record is a single agent and their information.
    AGENT_TABLE AS
    (
        SELECT
              oad.OFFERAGENTID                                                      AS "AGENT_ID"                   -- Primary Key and Agent ID
            , INITCAP(TRIM(TRIM(oad.FIRSTNAME) || ' ' || TRIM(oad.LASTNAME)))       AS "AGENT_FULL_NAME"            -- Concatenated full name of the agent placed in Camel Case.
            , LOWER(oad.EMAIL)                                                      AS "AGENT_EMAIL"                -- Lower-cased email of the agent.
            , INITCAP(oft.OFFERTEAMNAME)                                            AS "AGENT_TEAM"                 -- Name of the Agent Team in Camel Case
            , caq.NAME                                                              AS "AGENT_QUEUE"                -- Description of the Agent's queue.
        FROM STITCH.PEDDLE.TBL_OFFERAGENTDETAILS oad
        LEFT JOIN STITCH.PEDDLE.OFFERTEAM oft ON oad.OFFERTEAMID = oft.OFFERTEAMID
        LEFT JOIN STITCH.PEDDLE_OTHER.TBL_CALLQUEUE caq ON oad.QUEUE = caq.ID
    )

SELECT
      sdets.STATUS_HOUR_ID
    , sdets.STATUS_DATE
    , agt.AGENT_ID
    , agt.AGENT_FULL_NAME
    , agt.AGENT_EMAIL
    , agt.AGENT_TEAM
    , agt.AGENT_QUEUE
    , sdets.AGENT_STATUS
    , sdets.STATUS_CHANGE_BY
    , sdets.STATUS_START_AT
    , sdets.STATUS_END_AT
    , sdets.STATUS_DURATION_SECS
FROM STATUS_DETAILS sdets
LEFT JOIN AGENT_TABLE agt ON agt.AGENT_ID = sdets.AGENT_ID_FK
ORDER BY sdets.STATUS_DATE, agt.AGENT_ID, sdets.STATUS_START_AT
;


-- noinspection SqlConstantCondition

SELECT DISTINCT
      YEAR(stats.STATUS_START_AT)   * 100000000 +
      MONTH(stats.STATUS_START_AT)  * 1000000   +
      DAY(stats.STATUS_START_AT)    * 10000     +
      HOUR(stats.STATUS_START_AT)   * 100       +
      MINUTE(stats.STATUS_START_AT)                 AS "STAT_TIME_ID"
    , stats.STATUS_DATE
    , stats.AGENT_FULL_NAME
    , stats.AGENT_STATUS
    , stats.STATUS_CHANGE_BY
    , stats.STATUS_START_AT
    , stats.STATUS_END_AT
    , stats.STATUS_DURATION_SECS
    , twc.CALL_STATUS
    , twc.CALL_INCOMING_AT
    , twc.CALL_QUEUED_AT
    , twc.CALL_ACCEPTED_AT
    , twc.CALL_HANGUP_AT
    , twc.CALL_DURATION_SECS
    , twc.CALL_ANSWER_SPEED_SECS
    , twc.CALL_TALK_TIME_SECS
    , twc.PUBLISHER_NAME

FROM ANALYTICS.TABLAB_DEV.TWILIO_STATUSES stats
LEFT JOIN ANALYTICS.TABLAB_PROTO.TWILIO_CALLS twc
    ON twc.CALL_INCOMING_HOUR_ID = stats.STATUS_HOUR_ID
    AND twc.AGENT_ID = stats.AGENT_ID
    AND YEAR(stats.STATUS_START_AT)   * 100000000 +
        MONTH(stats.STATUS_START_AT)  * 1000000   +
        DAY(stats.STATUS_START_AT)    * 10000     +
        HOUR(stats.STATUS_START_AT)   * 100       +
        MINUTE(stats.STATUS_START_AT)             =

        YEAR(twc.CALL_INCOMING_AT)    * 100000000 +
        MONTH(twc.CALL_INCOMING_AT)   * 1000000   +
        DAY(twc.CALL_INCOMING_AT)     * 10000     +
        HOUR(twc.CALL_INCOMING_AT)    * 100       +
        MINUTE(twc.CALL_INCOMING_AT)

WHERE 1=1
AND STATUS_DATE = '2021-11-13'
AND HOUR(STATUS_START_AT) = 9
AND MINUTE(STATUS_START_AT) BETWEEN 0 AND 10
AND stats.AGENT_FULL_NAME = 'Alistair Fernandes'
ORDER BY STATUS_START_AT
LIMIT 100;
