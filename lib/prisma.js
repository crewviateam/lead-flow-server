// lib/prisma.js
// Prisma Client Singleton - ensures single connection pool across the application
// UPDATED: Added connection pool configuration for production scalability

const { PrismaClient } = require('@prisma/client');

// Prevent multiple instances in development due to hot reloading
const globalForPrisma = globalThis;

// Connection pool configuration for production
// These can be overridden via DATABASE_URL query parameters:
// postgresql://user:pass@host:5432/db?connection_limit=20&pool_timeout=30
const prismaConfig = {
  log: process.env.NODE_ENV === 'development' 
    ? ['query', 'info', 'warn', 'error'] 
    : ['error'],
  // Connection pool settings (Prisma defaults: connection_limit = num_cpus * 2 + 1)
  // For production with multiple instances, set via DATABASE_URL params
};

const prisma = globalForPrisma.prisma ?? new PrismaClient(prismaConfig);

if (process.env.NODE_ENV !== 'production') {
  globalForPrisma.prisma = prisma;
}

// Connection health check
async function checkDatabaseHealth() {
  try {
    await prisma.$queryRaw`SELECT 1`;
    return { healthy: true, message: 'Database connected' };
  } catch (error) {
    return { healthy: false, message: error.message };
  }
}

// Graceful shutdown helper
async function disconnectPrisma() {
  await prisma.$disconnect();
  console.log('✅ Prisma disconnected');
}

// Log connection pool info on startup
console.log(`[Prisma] Initialized with pool settings from DATABASE_URL`);
console.log(`[Prisma] Tip: Add ?connection_limit=20&pool_timeout=30 to DATABASE_URL for production`);

module.exports = { prisma, disconnectPrisma, checkDatabaseHealth };
