// controllers/TagController.js
// Tag controller using Prisma

const { LeadRepository } = require('../repositories');
const { prisma } = require('../lib/prisma');

class TagController {
  /**
   * GET /api/tags
   * Get all unique tags used across all leads
   */
  async getAllTags(req, res) {
    try {
      const result = await LeadRepository.getAllTags();
      res.status(200).json(result);
    } catch (error) {
      console.error('Get all tags error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * POST /api/leads/:id/tags
   * Add tags to a lead
   */
  async addTags(req, res) {
    try {
      const { id } = req.params;
      const { tags } = req.body;
      
      if (!tags || !Array.isArray(tags)) {
        return res.status(400).json({ error: 'Tags array is required' });
      }
      
      const newTags = tags.map(t => t.toLowerCase().trim()).filter(Boolean);
      const lead = await LeadRepository.addTags(id, newTags);
      
      res.status(200).json({
        message: `Added ${newTags.length} tag(s)`,
        tags: lead.tags
      });
    } catch (error) {
      if (error.message === 'Lead not found') {
        return res.status(404).json({ error: 'Lead not found' });
      }
      console.error('Add tags error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * DELETE /api/leads/:id/tags/:tag
   * Remove a tag from a lead
   */
  async removeTag(req, res) {
    try {
      const { id, tag } = req.params;
      
      const tagToRemove = tag.toLowerCase().trim();
      const lead = await LeadRepository.removeTags(id, [tagToRemove]);
      
      res.status(200).json({
        message: `Removed tag "${tagToRemove}"`,
        tags: lead.tags
      });
    } catch (error) {
      if (error.message === 'Lead not found') {
        return res.status(404).json({ error: 'Lead not found' });
      }
      console.error('Remove tag error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * POST /api/leads/bulk-tag
   * Add tags to multiple leads at once
   */
  async bulkAddTags(req, res) {
    try {
      const { leadIds, tags } = req.body;
      
      if (!leadIds || !Array.isArray(leadIds) || leadIds.length === 0) {
        return res.status(400).json({ error: 'leadIds array is required' });
      }
      if (!tags || !Array.isArray(tags) || tags.length === 0) {
        return res.status(400).json({ error: 'tags array is required' });
      }
      
      const normalizedTags = tags.map(t => t.toLowerCase().trim()).filter(Boolean);
      
      // For each lead, get current tags and add new ones
      let modifiedCount = 0;
      for (const leadId of leadIds) {
        try {
          const lead = await LeadRepository.findById(leadId);
          if (lead) {
            const existingTags = new Set(lead.tags || []);
            normalizedTags.forEach(t => existingTags.add(t));
            await prisma.lead.update({
              where: { id: parseInt(leadId) },
              data: { tags: Array.from(existingTags), updatedAt: new Date() }
            });
            modifiedCount++;
          }
        } catch (e) { /* skip invalid leads */ }
      }
      
      res.status(200).json({
        message: `Added ${normalizedTags.length} tag(s) to ${modifiedCount} leads`,
        modifiedCount
      });
    } catch (error) {
      console.error('Bulk add tags error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * POST /api/leads/bulk-untag
   * Remove tags from multiple leads at once
   */
  async bulkRemoveTags(req, res) {
    try {
      const { leadIds, tags } = req.body;
      
      if (!leadIds || !Array.isArray(leadIds) || leadIds.length === 0) {
        return res.status(400).json({ error: 'leadIds array is required' });
      }
      if (!tags || !Array.isArray(tags) || tags.length === 0) {
        return res.status(400).json({ error: 'tags array is required' });
      }
      
      const tagsToRemove = new Set(tags.map(t => t.toLowerCase().trim()).filter(Boolean));
      
      let modifiedCount = 0;
      for (const leadId of leadIds) {
        try {
          const lead = await LeadRepository.findById(leadId);
          if (lead) {
            const newTags = (lead.tags || []).filter(t => !tagsToRemove.has(t));
            await prisma.lead.update({
              where: { id: parseInt(leadId) },
              data: { tags: newTags, updatedAt: new Date() }
            });
            modifiedCount++;
          }
        } catch (e) { /* skip invalid leads */ }
      }
      
      res.status(200).json({
        message: `Removed ${tagsToRemove.size} tag(s) from ${modifiedCount} leads`,
        modifiedCount
      });
    } catch (error) {
      console.error('Bulk remove tags error:', error);
      res.status(500).json({ error: error.message });
    }
  }
}

module.exports = new TagController();
