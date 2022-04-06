# newts-stats

A tool to extract statistics about metrics and resources from Newts/Cassandra.

# Usage

It analyzes the `resource_metrics` from the `newts` keyspace, and display basic statistics.

It also creates two files, `metrics.txt` and `resources.txt` with the content from Cassandra for further analysis.

Pass `--help` to specify the Cassandra Hostname and the Newts Keyspace.
