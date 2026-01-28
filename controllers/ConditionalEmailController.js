// controllers/ConditionalEmailController.js
// API controller for managing conditional emails

const ConditionalEmailRepository = require('../repositories/ConditionalEmailRepository');
const ConditionalEmailService = require('../services/ConditionalEmailService');

class ConditionalEmailController {
  
  /**
   * GET /api/conditional-emails
   * Get all conditional emails
   */
  async getAll(req, res) {
    try {
      const conditionals = await ConditionalEmailRepository.findAll();
      const stats = await ConditionalEmailService.getStats();
      
      res.json({
        conditionalEmails: conditionals,
        stats
      });
    } catch (error) {
      console.error('Get conditional emails error:', error);
      res.status(500).json({ error: error.message });
    }
  }
  
  /**
   * GET /api/conditional-emails/:id
   * Get a single conditional email with recent jobs
   */
  async getById(req, res) {
    try {
      const { id } = req.params;
      const conditional = await ConditionalEmailRepository.findById(id);
      
      if (!conditional) {
        return res.status(404).json({ error: 'Conditional email not found' });
      }
      
      res.json(conditional);
    } catch (error) {
      console.error('Get conditional email error:', error);
      res.status(500).json({ error: error.message });
    }
  }
  
  /**
   * POST /api/conditional-emails
   * Create a new conditional email
   */
  async create(req, res) {
    try {
      const {
        name,
        description,
        triggerEvent,
        triggerStep,
        delayHours,
        templateId,
        cancelPending,
        priority,
        enabled
      } = req.body;
      
      // Validate required fields
      if (!name || !triggerEvent || !triggerStep) {
        return res.status(400).json({ 
          error: 'Missing required fields: name, triggerEvent, and triggerStep are required' 
        });
      }
      
      // Validate trigger event
      const validEvents = ['opened', 'clicked', 'delivered', 'bounced'];
      if (!validEvents.includes(triggerEvent)) {
        return res.status(400).json({ 
          error: `Invalid triggerEvent. Must be one of: ${validEvents.join(', ')}` 
        });
      }
      
      const conditional = await ConditionalEmailRepository.create({
        name,
        description,
        triggerEvent,
        triggerStep,
        delayHours: parseInt(delayHours) || 0,
        templateId: templateId || null,
        cancelPending: cancelPending !== false,
        priority: parseInt(priority) || 10,
        enabled: enabled !== false
      });
      
      console.log(`[ConditionalEmail] Created: ${name} (trigger: ${triggerEvent} on ${triggerStep})`);
      
      res.status(201).json(conditional);
    } catch (error) {
      console.error('Create conditional email error:', error);
      res.status(500).json({ error: error.message });
    }
  }
  
  /**
   * PUT /api/conditional-emails/:id
   * Update a conditional email
   */
  async update(req, res) {
    try {
      const { id } = req.params;
      const updateData = req.body;
      
      // Check if exists
      const existing = await ConditionalEmailRepository.findById(id);
      if (!existing) {
        return res.status(404).json({ error: 'Conditional email not found' });
      }
      
      const conditional = await ConditionalEmailRepository.update(id, updateData);
      
      console.log(`[ConditionalEmail] Updated: ${conditional.name}`);
      
      res.json(conditional);
    } catch (error) {
      console.error('Update conditional email error:', error);
      res.status(500).json({ error: error.message });
    }
  }
  
  /**
   * DELETE /api/conditional-emails/:id
   * Delete a conditional email
   */
  async delete(req, res) {
    try {
      const { id } = req.params;
      
      // Check if exists
      const existing = await ConditionalEmailRepository.findById(id);
      if (!existing) {
        return res.status(404).json({ error: 'Conditional email not found' });
      }
      
      await ConditionalEmailRepository.delete(id);
      
      console.log(`[ConditionalEmail] Deleted: ${existing.name}`);
      
      res.json({ message: 'Conditional email deleted successfully' });
    } catch (error) {
      console.error('Delete conditional email error:', error);
      res.status(500).json({ error: error.message });
    }
  }
  
  /**
   * PATCH /api/conditional-emails/:id/toggle
   * Toggle enabled status
   */
  async toggle(req, res) {
    try {
      const { id } = req.params;
      
      const conditional = await ConditionalEmailRepository.toggleEnabled(id);
      
      console.log(`[ConditionalEmail] Toggled ${conditional.name}: enabled=${conditional.enabled}`);
      
      res.json(conditional);
    } catch (error) {
      console.error('Toggle conditional email error:', error);
      res.status(500).json({ error: error.message });
    }
  }
  
  /**
   * GET /api/conditional-emails/trigger-options
   * Get available trigger step options for the dropdown
   */
  async getTriggerOptions(req, res) {
    try {
      const options = await ConditionalEmailService.getTriggerStepOptions();
      
      // Include standard steps
      const standardSteps = [
        { value: 'Initial Email', label: 'Initial Email' }
      ];
      
      res.json({
        events: [
          { value: 'opened', label: 'Email Opened' },
          { value: 'clicked', label: 'Link Clicked' },
          { value: 'delivered', label: 'Email Delivered' },
          { value: 'bounced', label: 'Email Bounced' }
        ],
        steps: [...standardSteps, ...options]
      });
    } catch (error) {
      console.error('Get trigger options error:', error);
      res.status(500).json({ error: error.message });
    }
  }
  
  /**
   * GET /api/conditional-emails/jobs/:leadId
   * Get conditional email jobs for a specific lead
   */
  async getJobsForLead(req, res) {
    try {
      const { leadId } = req.params;
      const jobs = await ConditionalEmailRepository.getJobsForLead(leadId);
      res.json(jobs);
    } catch (error) {
      console.error('Get jobs for lead error:', error);
      res.status(500).json({ error: error.message });
    }
  }
  
  /**
   * POST /api/conditional-emails/test-trigger
   * Manually test a trigger (for debugging)
   */
  async testTrigger(req, res) {
    try {
      const { leadId, eventType, sourceEmailType, sourceJobId } = req.body;
      
      if (!leadId || !eventType || !sourceEmailType) {
        return res.status(400).json({ 
          error: 'Missing required fields: leadId, eventType, sourceEmailType' 
        });
      }
      
      const triggeredJobs = await ConditionalEmailService.evaluateTriggers(
        leadId, 
        eventType, 
        sourceEmailType,
        sourceJobId
      );
      
      res.json({
        message: `Evaluated triggers for ${eventType} on ${sourceEmailType}`,
        triggeredJobs
      });
    } catch (error) {
      console.error('Test trigger error:', error);
      res.status(500).json({ error: error.message });
    }
  }
}

module.exports = new ConditionalEmailController();
