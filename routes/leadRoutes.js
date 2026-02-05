// routes/leadRoutes.js
// Lead management routes

const express = require('express');
const router = express.Router();
const { leadController, upload } = require('../controllers/LeadController');
const { validateBody, validateParams } = require('../middleware/validate');

// Lead CRUD
router.post('/upload-leads', upload.single('file'), leadController.uploadLeads.bind(leadController));
router.get('/', leadController.getLeads.bind(leadController));
router.get('/:id', validateParams('idParam'), async (req, res) => {
  try {
    const { LeadRepository, EmailJobRepository } = require('../repositories');
    
    const lead = await LeadRepository.findById(req.params.id, {
      include: { emailSchedule: true, eventHistory: true, manualMails: true }
    });
    if (!lead) {
      return res.status(404).json({ error: 'Lead not found' });
    }
    
    const emailJobs = await EmailJobRepository.findByLeadId(req.params.id);
    res.status(200).json({ lead, emailJobs });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});
router.put('/:id', validateParams('idParam'), validateBody('updateLead'), leadController.updateLead.bind(leadController));
router.delete('/:id', validateParams('idParam'), async (req, res) => {
  try {
    const { LeadRepository } = require('../repositories');
    const { prisma } = require('../lib/prisma');
    
    await prisma.emailJob.deleteMany({ where: { leadId: parseInt(req.params.id) } });
    await LeadRepository.delete(req.params.id);
    
    res.status(200).json({ message: 'Lead deleted successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Lead Actions
router.post('/schedule-emails', leadController.scheduleEmails.bind(leadController));
router.post('/:id/freeze', validateParams('idParam'), leadController.freezeLead.bind(leadController));
router.post('/:id/unfreeze', validateParams('idParam'), leadController.unfreezeLead.bind(leadController));
router.post('/:id/convert', validateParams('idParam'), leadController.convertLead.bind(leadController));
router.post('/:id/retry', validateParams('idParam'), leadController.retryLead.bind(leadController));
router.get('/:id/slots', validateParams('idParam'), leadController.getAvailableSlots.bind(leadController));

// Email Controls
router.post('/:id/manual-schedule', validateParams('idParam'), validateBody('scheduleManualSlot'), leadController.scheduleManualSlot.bind(leadController));
router.delete('/:id/email-jobs/:jobId', validateParams('idParam'), leadController.deleteEmailJob.bind(leadController));
router.post('/:id/pause', validateParams('idParam'), leadController.pauseFollowups.bind(leadController));
router.post('/:id/resume', validateParams('idParam'), leadController.resumeFollowups.bind(leadController));
router.post('/:id/skip', validateParams('idParam'), leadController.skipFollowup.bind(leadController));
router.post('/:id/revert-skip', validateParams('idParam'), leadController.revertSkipFollowup.bind(leadController));
router.delete('/:id/followup/:stepName', validateParams('idParam'), leadController.deleteFollowupFromLead.bind(leadController));

// Tags (lead-specific)
const TagController = require('../controllers/TagController');
router.post('/:id/tags', TagController.addTags.bind(TagController));
router.delete('/:id/tags/:tag', TagController.removeTag.bind(TagController));

module.exports = router;
