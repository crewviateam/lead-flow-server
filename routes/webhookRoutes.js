// routes/webhookRoutes.js
// External webhook routes

const express = require('express');
const router = express.Router();
const WebhookController = require('../controllers/WebhookController');

// Brevo webhooks (mounted at /brevo, so full path is /brevo/webhook)
router.post('/webhook', WebhookController.handleBrevoWebhook.bind(WebhookController));
router.get('/webhook', WebhookController.verifyWebhook.bind(WebhookController));

module.exports = router;
