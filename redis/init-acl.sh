#!/bin/sh
# Redis ACL initialization script

# Wait for Redis to be fully started
sleep 2

# Read password from secrets file
if [ -f "/run/secrets/redis_password" ]; then
    PASSWORD=$(cat /run/secrets/redis_password)
else
    echo "Error: Redis password file not found"
    exit 1
fi

# Connect to Redis and configure ACL users
redis-cli -h redis -p 6379 -a "$PASSWORD" << EOF
# Disable default user for security
ACL SETUSER default off

# Create streaming producer user with minimal permissions
ACL SETUSER streaming_producer on >$PASSWORD ~node_fills:* +ping +xadd +xlen

# Create streaming consumer user with minimal permissions  
ACL SETUSER streaming_consumer on >$PASSWORD ~node_fills:* +ping +xread +xlen

# Create admin user for maintenance if needed
ACL SETUSER admin on >$PASSWORD ~* &* +@all

# Save ACL configuration
ACL SAVE

# List users to confirm setup
ACL LIST
EOF

echo "Redis ACL configuration complete"