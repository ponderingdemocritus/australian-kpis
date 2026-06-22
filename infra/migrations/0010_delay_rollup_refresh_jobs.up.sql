-- Fresh Timescale installs schedule continuous-aggregate refresh jobs
-- immediately (`next_start = -infinity`). That can race with migration
-- rollback in verification environments and can also put avoidable load
-- on a just-started local stack. Keep the hourly cadence, but start it
-- after the stack has settled.
SELECT alter_job(job_id, next_start => now() + INTERVAL '1 hour')
FROM timescaledb_information.jobs
WHERE proc_name = 'policy_refresh_continuous_aggregate'
AND scheduled;
