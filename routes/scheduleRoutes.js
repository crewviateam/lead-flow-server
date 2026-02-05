// routes/scheduleRoutes.js
// Schedule and calendar routes

const express = require('express');
const router = express.Router();
const scheduleController = require('../controllers/ScheduleController');

router.get('/', scheduleController.getSchedule.bind(scheduleController));
router.get('/timezones', scheduleController.getTimezones.bind(scheduleController));

module.exports = router;
