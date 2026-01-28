// controllers/TemplateController.js
// Template controller using Prisma

const { TemplateRepository } = require('../repositories');

class TemplateController {
  // Get all templates
  async getAllTemplates(req, res) {
    try {
      const templates = await TemplateRepository.findAll();
      res.status(200).json(templates);
    } catch (error) {
      console.error('Error fetching templates:', error);
      res.status(500).json({ error: 'Failed to fetch templates' });
    }
  }

  // Get single template
  async getTemplate(req, res) {
    try {
      const template = await TemplateRepository.findById(parseInt(req.params.id));
      if (!template) {
        return res.status(404).json({ error: 'Template not found' });
      }
      res.status(200).json(template);
    } catch (error) {
      console.error('Error fetching template:', error);
      res.status(500).json({ error: 'Failed to fetch template' });
    }
  }

  // Create new template
  async createTemplate(req, res) {
    try {
      const { name, subject, body, variables } = req.body;
      const template = await TemplateRepository.create({
        name,
        subject,
        body,
        variables
      });
      res.status(201).json(template);
    } catch (error) {
      console.error('Error creating template:', error);
      res.status(400).json({ error: error.message });
    }
  }

  // Update template
  async updateTemplate(req, res) {
    try {
      const template = await TemplateRepository.update(
        parseInt(req.params.id),
        req.body
      );
      if (!template) {
        return res.status(404).json({ error: 'Template not found' });
      }
      res.status(200).json(template);
    } catch (error) {
      console.error('Error updating template:', error);
      res.status(400).json({ error: error.message });
    }
  }

  // Delete template
  async deleteTemplate(req, res) {
    try {
      const deleted = await TemplateRepository.delete(parseInt(req.params.id));
      if (!deleted) {
        return res.status(404).json({ error: 'Template not found' });
      }
      res.status(200).json({ message: 'Template deleted' });
    } catch (error) {
      console.error('Error deleting template:', error);
      res.status(500).json({ error: 'Failed to delete template' });
    }
  }
}

module.exports = new TemplateController();
