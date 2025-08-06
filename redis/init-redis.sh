#!/bin/sh
# Redis initialization script with ACL setup
# This runs Redis server with automatic ACL configuration

# Validate required environment variables
if [ -z "$REDIS_PASSWORD" ]; then
    echo "ERROR: REDIS_PASSWORD environment variable is required"
    echo "Please ensure you're running with: docker compose --env-file .env up"
    exit 1
fi

# Generate Redis configuration with current environment variables
echo "Generating Redis configuration..."
sh /usr/local/bin/generate-config.sh

# Start Redis server in background
redis-server /usr/local/etc/redis/redis.conf &
REDIS_PID=$!

# Wait for Redis to be ready
echo "Waiting for Redis to start..."
while ! redis-cli -a "$REDIS_PASSWORD" ping >/dev/null 2>&1; do
    sleep 1
done

echo "Redis started, configuring ACL users..."

# Configure ACL users
redis-cli -a "$REDIS_PASSWORD" << EOF
ACL SETUSER streaming_producer on >$REDIS_PASSWORD ~node_fills:* +ping +xadd +xlen
ACL SETUSER streaming_consumer on >$REDIS_PASSWORD ~node_fills:* +ping +xread +xlen
ACL SETUSER admin on >$REDIS_PASSWORD ~* &* +@all
ACL SAVE
EOF

echo "ACL configuration complete. Users created:"
redis-cli -a "$REDIS_PASSWORD" ACL LIST

# Keep Redis running in foreground
wait $REDIS_PID