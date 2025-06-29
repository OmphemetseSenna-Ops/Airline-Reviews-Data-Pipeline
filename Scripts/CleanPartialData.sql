-- Clean up any partial data from previous failed runs
DELETE FROM fact.Reviews WHERE BatchID = 1;
DELETE FROM dim.FlightDetails;
DELETE FROM dim.Author;
DELETE FROM audit.ETLBatch WHERE BatchID = 1;


-- Clean up any partial data
DELETE FROM fact.Reviews WHERE BatchID = 3;
DELETE FROM dim.FlightDetails;
DELETE FROM dim.Author;
DELETE FROM audit.ETLBatch WHERE BatchID = 3;