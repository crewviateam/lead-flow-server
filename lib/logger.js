// lib/logger.js
// Centralized structured logging using Pino
// Production: JSON output for log aggregation
// Development: Pretty-printed for readability

const pino = require("pino");

// Create the logger with environment-aware configuration
const logger = pino({
  level: process.env.LOG_LEVEL || "info",

  // Base fields included in every log
  base: {
    service: "lead-email-system",
    env: process.env.NODE_ENV || "development",
  },

  // ISO timestamp format
  timestamp: pino.stdTimeFunctions.isoTime,

  // Pretty print in development
  transport:
    process.env.NODE_ENV !== "production"
      ? {
          target: "pino-pretty",
          options: {
            colorize: true,
            translateTime: "SYS:standard",
            ignore: "pid,hostname,service,env",
          },
        }
      : undefined,

  // Redact sensitive fields
  redact: {
    paths: ["req.headers.authorization", "password", "apiKey", "brevoApiKey"],
    censor: "[REDACTED]",
  },
});

/**
 * Create a child logger with additional context
 * Usage: const log = logger.child({ component: 'EmailWorker' })
 */
const createLogger = (context) => {
  return logger.child(context);
};

// Pre-configured loggers for common components
const loggers = {
  default: logger,
  worker: createLogger({ component: "worker" }),
  scheduler: createLogger({ component: "scheduler" }),
  api: createLogger({ component: "api" }),
  cache: createLogger({ component: "cache" }),
  events: createLogger({ component: "events" }),
  analytics: createLogger({ component: "analytics" }),
  webhook: createLogger({ component: "webhook" }),
  service: createLogger({ component: "service" }),
  email: createLogger({ component: "email" }),
};

module.exports = {
  logger,
  createLogger,
  loggers,
};
