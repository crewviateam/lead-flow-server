// middleware/index.js
// Central export for all middleware

const { apiLimiter, strictLimiter, uploadLimiter } = require('./rateLimiter');
const { validateBody, validateQuery, validateParams, schemas } = require('./validate');

module.exports = {
  // Rate limiting
  apiLimiter,
  strictLimiter,
  uploadLimiter,
  
  // Validation
  validateBody,
  validateQuery,
  validateParams,
  schemas
};
