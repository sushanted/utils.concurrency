# utils.concurrency
All concurrency related utils

PartitionedConcurrencyManager : Limit concurrent tasks per partition based on id. For example, if there are multiple orgs, at most N tasks can be allowed to be run concurrently for any org. As soon as a task for an org is complete, another pending task for the same org is allowed to run.
