// routes/analyticsRoutes.js
// Analytics and reporting routes

const express = require('express');
const router = express.Router();
const AnalyticsController = require('../controllers/AnalyticsController');

router.get('/summary', AnalyticsController.getSummary.bind(AnalyticsController));
router.get('/dashboard', AnalyticsController.getDashboardStats.bind(AnalyticsController));
router.get('/email-jobs', AnalyticsController.getEmailJobStats.bind(AnalyticsController));
router.get('/leads', AnalyticsController.getLeadStats.bind(AnalyticsController));
router.get('/breakdown', AnalyticsController.getDetailedBreakdown.bind(AnalyticsController));
router.get('/hierarchy', AnalyticsController.getHierarchicalAnalytics.bind(AnalyticsController));
router.get('/recent-activity', AnalyticsController.getRecentActivity.bind(AnalyticsController));
router.post('/sync', AnalyticsController.syncFromBrevo.bind(AnalyticsController));

module.exports = router;
