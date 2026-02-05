// routes/terminalStatesRoutes.js
// Terminal states management (dead, unsubscribed, complaint)

const express = require('express');
const router = express.Router();
const TerminalStatesController = require('../controllers/TerminalStatesController');

router.get('/', TerminalStatesController.getLeadsByState.bind(TerminalStatesController));
router.get('/stats', TerminalStatesController.getStats.bind(TerminalStatesController));
router.get('/:id', TerminalStatesController.getLeadDetails.bind(TerminalStatesController));
router.post('/:id/resurrect', TerminalStatesController.resurrect.bind(TerminalStatesController));

module.exports = router;
