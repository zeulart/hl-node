#!/bin/sh
# Generate Redis configuration with environment variables
set -e  # Exit on any error

# Validate required environment variables
if [ -z "$REDIS_PASSWORD" ]; then
    echo "ERROR: REDIS_PASSWORD environment variable is required"
    echo "Please ensure you're running with: docker compose --env-file .env up"
    exit 1
fi

# Create config directory if it doesn't exist
mkdir -p /usr/local/etc/redis || {
    echo "ERROR: Cannot create Redis config directory"
    exit 1
}

cat > /usr/local/etc/redis/redis.conf << EOF
# Redis Security Configuration for Hyperliquid Streaming
# Network exposure with strong security measures

# ======================================================================
# NETWORK CONFIGURATION
# ======================================================================
# Bind to all interfaces for external Docker access
bind 0.0.0.0
port 6379
protected-mode yes

# Connection limits and timeouts
maxclients 1000
timeout 300
tcp-keepalive 60
tcp-backlog 511

# ======================================================================
# AUTHENTICATION & SECURITY
# ======================================================================
requirepass $REDIS_PASSWORD

# Disable dangerous commands that could compromise system
rename-command FLUSHDB ""
rename-command FLUSHALL ""
rename-command DEBUG ""
rename-command EVAL ""
rename-command SCRIPT ""
rename-command CONFIG ""
rename-command SHUTDOWN SHUTDOWN_hyperliquid_2024
rename-command DEL DEL_restricted
rename-command KEYS KEYS_restricted

# ======================================================================
# MEMORY MANAGEMENT  
# ======================================================================
maxmemory 8gb
maxmemory-policy allkeys-lru

# Disable persistence for performance (as per original config)
save ""
appendonly no

# ======================================================================
# PERFORMANCE TUNING
# ======================================================================
# Client output buffer limits (important for streams)
client-output-buffer-limit normal 0 0 0
client-output-buffer-limit replica 256mb 64mb 60
client-output-buffer-limit pubsub 32mb 8mb 60

# Memory optimization
hash-max-ziplist-entries 512
hash-max-ziplist-value 64
list-max-ziplist-size -2
set-max-intset-entries 512
zset-max-ziplist-entries 128
zset-max-ziplist-value 64

# Enable lazy freeing
lazyfree-lazy-eviction yes
lazyfree-lazy-expire yes
lazyfree-lazy-server-del yes

# Stream configuration
stream-node-max-bytes 4096
stream-node-max-entries 100

# ======================================================================
# LOGGING
# ======================================================================
loglevel notice
logfile ""
syslog-enabled no

# ======================================================================
# SLOW LOG
# ======================================================================
slowlog-log-slower-than 10000
slowlog-max-len 128
EOF