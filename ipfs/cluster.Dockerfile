FROM ipfs/kubo:v0.43.0

COPY private-swarm-entrypoint.sh /private-swarm-entrypoint.sh
RUN chmod 0755 /private-swarm-entrypoint.sh

ENTRYPOINT ["/private-swarm-entrypoint.sh"]
