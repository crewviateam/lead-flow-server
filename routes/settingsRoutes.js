// routes/settingsRoutes.js
// Settings and configuration routes

const express = require('express');
const router = express.Router();
const SettingsController = require('../controllers/SettingsController');

// General settings
router.get('/', SettingsController.getSettings.bind(SettingsController));
router.put('/', SettingsController.updateSettings.bind(SettingsController));

// Followup configuration
router.get('/followups', SettingsController.getFollowups.bind(SettingsController));
router.post('/followups', SettingsController.addFollowup.bind(SettingsController));
router.put('/followups/:id', SettingsController.updateFollowup.bind(SettingsController));
router.delete('/followups/:id', SettingsController.deleteFollowup.bind(SettingsController));
router.post('/followups/reorder', SettingsController.reorderFollowups.bind(SettingsController));

// Pause dates and weekends
router.get('/paused-dates', SettingsController.getPausedDates.bind(SettingsController));
router.post('/pause-date', SettingsController.pauseDate.bind(SettingsController));
router.post('/unpause-date', SettingsController.unpauseDate.bind(SettingsController));
router.post('/weekend-days', SettingsController.updateWeekendDays.bind(SettingsController));
router.post('/reschedule-paused', SettingsController.reschedulePausedEmails.bind(SettingsController));

// Brevo connection
router.post('/test-brevo', SettingsController.testBrevoConnection.bind(SettingsController));
router.post('/clear-logs', SettingsController.clearBrevoLogs.bind(SettingsController));

// Rulebook
router.get('/rulebook', SettingsController.getRulebook.bind(SettingsController));
router.put('/rulebook', SettingsController.updateRulebook.bind(SettingsController));
router.post('/rulebook/reset', SettingsController.resetRulebook.bind(SettingsController));
router.get('/rulebook/defaults', SettingsController.getDefaultRulebook.bind(SettingsController));
router.get('/rulebook/permissions', SettingsController.getMailTypePermissions.bind(SettingsController));

module.exports = router;
