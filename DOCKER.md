# Docker Deployment Guide

## Redis Security Configuration

This project uses Redis with authentication and ACL (Access Control Lists) for secure stream processing.

### Environment Configuration

1. **Create a `.env` file** in the project root with your Redis password:
   ```bash
   # Generate a secure password
   openssl rand -base64 32
   
   # Add to .env file
   echo "REDIS_PASSWORD=your_generated_password_here" > .env
   ```

2. **Verify `.env` is gitignored** (already configured):
   ```bash
   cat .gitignore | grep .env
   ```

### Running Services

Always use the `--env-file` flag when running Docker Compose to ensure secure credential handling:

```bash
# Start all services
docker compose --env-file .env up -d

# Start specific service
docker compose --env-file .env up -d redis

# View logs
docker compose --env-file .env logs -f

# Stop services
docker compose --env-file .env down
```

### Security Notes

- **Never use plain environment variables** in docker-compose.yml as they can be exposed via `docker ps` and container inspection
- **Always use `--env-file`** flag to inject environment variables securely at runtime
- The `.env` file is gitignored and should never be committed to the repository
- Redis is configured with:
  - Password authentication required
  - ACL users with role-based permissions (producer, consumer, admin)
  - Dangerous commands disabled (FLUSHDB, FLUSHALL, etc.)
  - Memory limits and eviction policies

### Redis ACL Users

The system automatically creates three ACL users on startup:

1. **streaming_producer**: Can write to streams (XADD)
2. **streaming_consumer**: Can read from streams (XREAD)
3. **admin**: Full access for administrative tasks

### Verification Steps

After starting services, verify the configuration:

```bash
# Check service status
docker compose --env-file .env ps

# Verify Redis authentication
docker exec hyperliquid-redis-1 redis-cli -a 'your_password_here' ping

# Check ACL users were created
docker exec hyperliquid-redis-1 redis-cli -a 'your_password_here' ACL LIST

# Test ACL user authentication
docker exec hyperliquid-redis-1 redis-cli --user streaming_producer -a 'your_password_here' ping

# Check stream operations
docker exec hyperliquid-redis-1 redis-cli --user streaming_producer -a 'your_password_here' XLEN node_fills:ALL
```

### Clean Restart

To perform a clean restart of services:

```bash
# Stop services
docker compose --env-file .env stop redis log-streamer

# Remove containers
docker compose --env-file .env rm -f redis log-streamer

# Start fresh
docker compose --env-file .env up -d redis log-streamer

# Check logs
docker compose --env-file .env logs redis log-streamer --tail=50
```

### Troubleshooting

#### Authentication Errors
- **Symptom**: `AUTH failed: WRONGPASS invalid username-password pair`
- **Solution**: 
  1. Ensure the `.env` file exists and contains `REDIS_PASSWORD`
  2. Verify no extra spaces or quotes in the password
  3. Always use `--env-file .env` with docker compose commands

#### ACL SAVE Warning
- **Symptom**: `ERR This Redis instance is not configured to use an ACL file`
- **Note**: This warning is expected and safe to ignore. ACLs are configured dynamically on startup rather than using ACL files.

#### Environment Variable Not Found
- **Symptom**: `ERROR: REDIS_PASSWORD environment variable is required`
- **Solution**: 
  1. Check `.env` file exists in project root
  2. Verify format: `REDIS_PASSWORD=your_password` (no spaces around =)
  3. Use `docker compose --env-file .env` not just `docker compose`

#### Services Not Connecting
- **Symptom**: Streaming service can't connect to Redis
- **Solution**:
  1. Ensure Redis is fully started before dependent services
  2. Check Redis logs: `docker compose --env-file .env logs redis`
  3. Verify network connectivity between containers
  4. Confirm password matches in all services

### Monitoring

Monitor Redis and streaming services:

```bash
# Real-time logs
docker compose --env-file .env logs -f redis log-streamer

# Check Redis memory usage
docker exec hyperliquid-redis-1 redis-cli -a 'your_password_here' INFO memory | grep used_memory_human

# Monitor stream sizes
docker exec hyperliquid-redis-1 redis-cli -a 'your_password_here' --user streaming_producer XINFO STREAM node_fills:ALL
```