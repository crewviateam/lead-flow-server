// routes/emailJobRoutes.js
// Email job management routes

const express = require('express');
const router = express.Router();
const EmailJobController = require('../controllers/EmailJobController');

router.get('/', EmailJobController.getEmailJobs.bind(EmailJobController));
router.get('/:id', EmailJobController.getEmailJob.bind(EmailJobController));
router.post('/:id/retry', EmailJobController.retryJob.bind(EmailJobController));
router.post('/:id/resume', EmailJobController.resumeJob.bind(EmailJobController));
router.put('/:id/reschedule', EmailJobController.rescheduleJob.bind(EmailJobController));
router.delete('/:id', EmailJobController.cancelJob.bind(EmailJobController));

module.exports = router;
