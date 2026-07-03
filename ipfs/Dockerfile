FROM ipfs/kubo:latest

# Copy our custom entrypoint that applies optimized config on first init.
COPY entrypoint.sh /custom-entrypoint.sh
RUN chmod +x /custom-entrypoint.sh

ENTRYPOINT ["/custom-entrypoint.sh"]
