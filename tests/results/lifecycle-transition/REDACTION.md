# Evidence path redaction

Machine-specific repository prefixes are represented as `[REPO]`; other paths
under the executing user's profile are represented as `[USER_HOME]`. This changes
only machine-path presentation in logs and summaries. Test output, statuses,
counts, exit codes, failure history, the recorded PASS HEAD, source-content
digests, and every relative path in `inputs.manifest.json` are unchanged.
