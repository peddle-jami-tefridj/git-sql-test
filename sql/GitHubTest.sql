WITH MAX_AUCTION_RUN AS
    (SELECT
           cb.CLAIMID
         , cb.EXPERIMENTMAPPINGID
         , cb.ACCEPTABLEBIDAMOUNT
         , cb.ACTIONPERFORMED
         , cb.AUCTIONRUN
         , cb.AUCTIONDATE
         , cb.COUNTERBIDNAME
         , cb.DESIREDBIDAMOUNT
         , cb.CODESNIPPETNUMBER
         , cb.AUCTIONDATE :: DATE AS auction_on
         , cb.CREATEDDATE :: DATE AS created_on
         , cb.SALESFORCEID
         , row_number() over(partition by CLAIMID order by AUCTIONRUN desc) AS max_auction_run
    FROM STITCH.PEDDLE_OTHER.COUNTERBID CB
    ORDER BY CLAIMID, AUCTIONRUN DESC
    )

// As of 12-3-21:
    -- Distinct ClaimID WHERE CODESNIPPETNUMBER = 2.6 results --> 28,420 IDs.
SELECT
    COUNT(DISTINCT cb.CLAIMID )
FROM MAX_AUCTION_RUN cb
LEFT JOIN STITCH.SALESFORCE.OPPORTUNITY sf ON sf.ID = cb.SALESFORCEID
WHERE 1=1
-- This below filter here to MAX_AUCTION_RUN = 1 yields 1,248 CLAIMIDs. If you remove it, you will get the desired 28,420.
AND cb.MAX_AUCTION_RUN = 1
AND cb.CODESNIPPETNUMBER = '2.6';