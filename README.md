# MTA-to-DCH
This takes price signal data, calculates the DR flag based on the RRP in the price signal, and shoots it off to a DCH DRF data point

You should use credentials for the database without the proxy if you're backfilling with the DRF, otherwise use the proxy credentials with the lambda function that is scheduled to run every half an hour.
