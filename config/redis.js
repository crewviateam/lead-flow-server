// config/redis.js
// Redis connection configuration for BullMQ and distributed locking
// UPDATED: Added Redis Cluster support for 10,000+ concurrent users

const Redis = require('ioredis');

// Determine if using Redis Cluster (set REDIS_CLUSTER_NODES env var)
// Format: "host1:port1,host2:port2,host3:port3"
const clusterNodes = process.env.REDIS_CLUSTER_NODES;

let redisConnection;

if (clusterNodes) {
  // ========================================
  // REDIS CLUSTER MODE
  // For 10,000+ concurrent users
  // ========================================
  const nodes = clusterNodes.split(',').map(node => {
    const [host, port] = node.trim().split(':');
    return { host, port: parseInt(port) || 6379 };
  });

  redisConnection = new Redis.Cluster(nodes, {
    redisOptions: {
      password: process.env.REDIS_PASSWORD || undefined,
      maxRetriesPerRequest: null,
      enableReadyCheck: false,
    },
    clusterRetryStrategy: (times) => {
      if (times > 10) {
        console.error('[Redis Cluster] Max retry attempts reached');
        return null;
      }
      return Math.min(times * 500, 5000);
    },
    // Enable read from replicas for better read performance
    scaleReads: 'slave',
  });

  redisConnection.on('node error', (error, node) => {
    console.error(`[Redis Cluster] Node ${node} error:`, error.message);
  });

  console.log(`[Redis] Cluster mode enabled with ${nodes.length} nodes`);

} else {
  // ========================================
  // SINGLE REDIS INSTANCE MODE
  // For up to ~5,000 concurrent users
  // ========================================
  redisConnection = new Redis({
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT) || 6379,
    password: process.env.REDIS_PASSWORD || undefined,
    db: parseInt(process.env.REDIS_DB) || 0,
    
    // BullMQ requires these settings
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
    
    // Connection pooling
    lazyConnect: false,
    
    // Retry strategy for connection failures
    retryStrategy: (times) => {
      if (times > 10) {
        console.error('[Redis] Max retry attempts reached. Connection failed.');
        return null;
      }
      const delay = Math.min(times * 500, 5000);
      console.log(`[Redis] Retrying connection in ${delay}ms (attempt ${times})`);
      return delay;
    },
    
    // Reconnect on error for non-critical errors
    reconnectOnError: (err) => {
      const targetErrors = ['READONLY', 'ECONNRESET', 'ETIMEDOUT'];
      return targetErrors.some(e => err.message.includes(e));
    }
  });

  console.log('[Redis] Single instance mode');
}

// Common event handlers
redisConnection.on('connect', () => {
  console.log('✅ Redis connected');
});

redisConnection.on('ready', () => {
  console.log('✅ Redis ready for commands');
});

redisConnection.on('error', (err) => {
  console.error('[Redis] Connection error:', err.message);
});

redisConnection.on('close', () => {
  console.warn('[Redis] Connection closed');
});

redisConnection.on('reconnecting', () => {
  console.log('[Redis] Reconnecting...');
});

// Health check function
async function checkRedisHealth() {
  try {
    const pong = await redisConnection.ping();
    return { healthy: pong === 'PONG', message: 'Redis connected' };
  } catch (error) {
    return { healthy: false, message: error.message };
  }
}

// Graceful shutdown helper
async function disconnectRedis() {
  try {
    await redisConnection.quit();
    console.log('✅ Redis disconnected gracefully');
  } catch (error) {
    console.error('[Redis] Error during disconnect:', error.message);
    redisConnection.disconnect();
  }
}

module.exports = redisConnection;
module.exports.disconnectRedis = disconnectRedis;
module.exports.checkRedisHealth = checkRedisHealth;