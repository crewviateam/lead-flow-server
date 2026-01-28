// middleware/validate.js
// Input validation middleware using Joi
// Validates request body, query params, and route params

const Joi = require('joi');

/**
 * Validation schemas for API endpoints
 * Add new schemas here as needed
 */
const schemas = {
  // Lead endpoints
  createLead: Joi.object({
    name: Joi.string().required().max(255).trim(),
    email: Joi.string().email().required().lowercase().trim(),
    country: Joi.string().max(100).allow('', null),
    city: Joi.string().max(100).allow('', null),
    timezone: Joi.string().max(50).default('UTC'),
    tags: Joi.array().items(Joi.string().max(50)).default([])
  }),

  updateLead: Joi.object({
    name: Joi.string().max(255).trim(),
    email: Joi.string().email().lowercase().trim(),
    country: Joi.string().max(100).allow('', null),
    city: Joi.string().max(100).allow('', null),
    timezone: Joi.string().max(50),
    tags: Joi.array().items(Joi.string().max(50)),
    status: Joi.string().max(100)
  }).min(1), // At least one field required

  // Email job endpoints
  rescheduleJob: Joi.object({
    scheduledFor: Joi.date().iso(),
    newTime: Joi.date().iso(),
    newScheduledFor: Joi.date().iso()
  }).or('scheduledFor', 'newTime', 'newScheduledFor'),

  scheduleManualSlot: Joi.object({
    time: Joi.date().iso().required(),
    emailType: Joi.string().max(100).allow(null),
    title: Joi.string().max(255).allow(null, ''),
    templateId: Joi.number().integer().positive().allow(null),
    body: Joi.string().allow(null, '')
  }),

  // Settings endpoints
  updateSettings: Joi.object({
    rateLimitEmailsPerWindow: Joi.number().integer().min(1).max(100),
    rateLimitWindowMinutes: Joi.number().integer().min(1).max(60),
    businessHoursStart: Joi.number().integer().min(0).max(23),
    businessHoursEnd: Joi.number().integer().min(0).max(23),
    weekendDays: Joi.array().items(Joi.number().integer().min(0).max(6)),
    retryMaxAttempts: Joi.number().integer().min(1).max(10),
    retrySoftBounceDelayHrs: Joi.number().integer().min(1).max(72)
  }),

  // Template endpoints
  createTemplate: Joi.object({
    name: Joi.string().required().max(255).trim(),
    subject: Joi.string().required().max(500).trim(),
    body: Joi.string().required(),
    variables: Joi.array().items(Joi.string().max(50)).default([]),
    isDefault: Joi.boolean().default(false)
  }),

  // Pagination query params
  pagination: Joi.object({
    page: Joi.number().integer().min(1).default(1),
    limit: Joi.number().integer().min(1).max(100).default(50),
    sortBy: Joi.string().valid('createdAt', 'name', 'score', 'updatedAt').default('createdAt'),
    sortOrder: Joi.string().valid('asc', 'desc').default('desc')
  }),

  // ID param validation
  idParam: Joi.object({
    id: Joi.number().integer().positive().required()
  })
};

/**
 * Middleware factory for request body validation
 * @param {string} schemaName - Name of schema from schemas object
 * @returns {Function} Express middleware
 */
const validateBody = (schemaName) => {
  return (req, res, next) => {
    const schema = schemas[schemaName];
    if (!schema) {
      return res.status(500).json({ error: `Validation schema '${schemaName}' not found` });
    }

    const { error, value } = schema.validate(req.body, {
      abortEarly: false,      // Return all errors, not just first
      stripUnknown: true,     // Remove unknown fields
      convert: true           // Type coercion
    });

    if (error) {
      const errors = error.details.map(d => ({
        field: d.path.join('.'),
        message: d.message
      }));
      return res.status(400).json({ 
        error: 'Validation failed',
        details: errors
      });
    }

    req.body = value; // Use validated/sanitized values
    next();
  };
};

/**
 * Middleware factory for query params validation
 * @param {string} schemaName - Name of schema from schemas object
 */
const validateQuery = (schemaName) => {
  return (req, res, next) => {
    const schema = schemas[schemaName];
    if (!schema) {
      return res.status(500).json({ error: `Validation schema '${schemaName}' not found` });
    }

    const { error, value } = schema.validate(req.query, {
      abortEarly: false,
      stripUnknown: true,
      convert: true
    });

    if (error) {
      const errors = error.details.map(d => ({
        field: d.path.join('.'),
        message: d.message
      }));
      return res.status(400).json({ 
        error: 'Invalid query parameters',
        details: errors
      });
    }

    req.query = value;
    next();
  };
};

/**
 * Middleware factory for route params validation
 * @param {string} schemaName - Name of schema from schemas object
 */
const validateParams = (schemaName) => {
  return (req, res, next) => {
    const schema = schemas[schemaName];
    if (!schema) {
      return res.status(500).json({ error: `Validation schema '${schemaName}' not found` });
    }

    const { error, value } = schema.validate(req.params, {
      abortEarly: false,
      convert: true
    });

    if (error) {
      const errors = error.details.map(d => ({
        field: d.path.join('.'),
        message: d.message
      }));
      return res.status(400).json({ 
        error: 'Invalid route parameters',
        details: errors
      });
    }

    req.params = value;
    next();
  };
};

module.exports = {
  schemas,
  validateBody,
  validateQuery,
  validateParams
};
