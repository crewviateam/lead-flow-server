// lib/errors.js
// Centralized error classes for consistent error handling
// Use these throughout the application for predictable error responses

/**
 * Base application error class
 * All custom errors should extend this
 */
class AppError extends Error {
  constructor(message, statusCode = 500, code = 'INTERNAL_ERROR') {
    super(message);
    this.name = this.constructor.name;
    this.statusCode = statusCode;
    this.code = code;
    this.isOperational = true; // Distinguishes operational errors from programming errors
    Error.captureStackTrace(this, this.constructor);
  }

  toJSON() {
    return {
      error: this.message,
      code: this.code,
      ...(process.env.NODE_ENV === 'development' && { stack: this.stack })
    };
  }
}

/**
 * 400 Bad Request - Invalid input data
 */
class ValidationError extends AppError {
  constructor(message, details = null) {
    super(message, 400, 'VALIDATION_ERROR');
    this.details = details;
  }

  toJSON() {
    return {
      ...super.toJSON(),
      ...(this.details && { details: this.details })
    };
  }
}

/**
 * 404 Not Found - Resource doesn't exist
 */
class NotFoundError extends AppError {
  constructor(resource = 'Resource') {
    super(`${resource} not found`, 404, 'NOT_FOUND');
    this.resource = resource;
  }
}

/**
 * 409 Conflict - Duplicate or conflicting resource
 */
class ConflictError extends AppError {
  constructor(message = 'Resource already exists') {
    super(message, 409, 'CONFLICT');
  }
}

/**
 * 401 Unauthorized - Authentication required
 */
class UnauthorizedError extends AppError {
  constructor(message = 'Authentication required') {
    super(message, 401, 'UNAUTHORIZED');
  }
}

/**
 * 403 Forbidden - Insufficient permissions
 */
class ForbiddenError extends AppError {
  constructor(message = 'Access denied') {
    super(message, 403, 'FORBIDDEN');
  }
}

/**
 * 429 Too Many Requests - Rate limit exceeded
 */
class RateLimitError extends AppError {
  constructor(retryAfter = 60) {
    super('Too many requests, please slow down', 429, 'RATE_LIMIT_EXCEEDED');
    this.retryAfter = retryAfter;
  }

  toJSON() {
    return {
      ...super.toJSON(),
      retryAfter: this.retryAfter
    };
  }
}

/**
 * 503 Service Unavailable - External service failure
 */
class ServiceUnavailableError extends AppError {
  constructor(service = 'External service') {
    super(`${service} is temporarily unavailable`, 503, 'SERVICE_UNAVAILABLE');
    this.service = service;
  }
}

/**
 * Database operation failed
 */
class DatabaseError extends AppError {
  constructor(message = 'Database operation failed') {
    super(message, 500, 'DATABASE_ERROR');
  }
}

/**
 * Email sending failed
 */
class EmailError extends AppError {
  constructor(message = 'Failed to send email', originalError = null) {
    super(message, 500, 'EMAIL_ERROR');
    this.originalError = originalError;
  }
}

module.exports = {
  AppError,
  ValidationError,
  NotFoundError,
  ConflictError,
  UnauthorizedError,
  ForbiddenError,
  RateLimitError,
  ServiceUnavailableError,
  DatabaseError,
  EmailError
};
