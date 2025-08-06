#!/bin/sh
# Redis initialization script with ACL setup
# This runs Redis server with automatic ACL configuration

# Start Redis server in background
redis-server /usr/local/etc/redis/redis.conf &
REDIS_PID=$!

# Wait for Redis to be ready
echo "Waiting for Redis to start..."
while ! redis-cli -a "CHANGE_ME_GENERATE_STRONG_PASSWORD_HERE" ping >/dev/null 2>&1; do
    sleep 1
done

echo "Redis started, configuring ACL users..."

# Configure ACL users
redis-cli -a "CHANGE_ME_GENERATE_STRONG_PASSWORD_HERE" << 'EOF'
ACL SETUSER streaming_producer on >CHANGE_ME_GENERATE_STRONG_PASSWORD_HERE ~node_fills:* +ping +xadd +xlen
ACL SETUSER streaming_consumer on >CHANGE_ME_GENERATE_STRONG_PASSWORD_HERE ~node_fills:* +ping +xread +xlen
ACL SETUSER admin on >CHANGE_ME_GENERATE_STRONG_PASSWORD_HERE ~* &* +@all
CONFIG REWRITE
EOF

echo "ACL configuration complete. Users created:"
redis-cli -a "CHANGE_ME_GENERATE_STRONG_PASSWORD_HERE" ACL LIST

# Keep Redis running in foreground
wait $REDIS_PID