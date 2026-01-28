// routes/conditionalEmailRoutes.js
// Routes for conditional email management

const express = require('express');
const router = express.Router();
const ConditionalEmailController = require('../controllers/ConditionalEmailController');

// Trigger options (for UI dropdowns) - must be before /:id
router.get('/trigger-options', (req, res) => ConditionalEmailController.getTriggerOptions(req, res));

// Test trigger endpoint
router.post('/test-trigger', (req, res) => ConditionalEmailController.testTrigger(req, res));

// Jobs for a specific lead
router.get('/jobs/:leadId', (req, res) => ConditionalEmailController.getJobsForLead(req, res));

// CRUD operations
router.get('/', (req, res) => ConditionalEmailController.getAll(req, res));
router.get('/:id', (req, res) => ConditionalEmailController.getById(req, res));
router.post('/', (req, res) => ConditionalEmailController.create(req, res));
router.put('/:id', (req, res) => ConditionalEmailController.update(req, res));
router.delete('/:id', (req, res) => ConditionalEmailController.delete(req, res));

// Toggle enabled status
router.patch('/:id/toggle', (req, res) => ConditionalEmailController.toggle(req, res));

module.exports = router;
