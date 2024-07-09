const db = require('../db');

const getProcessorName = async (processorId) => {
  const result = await db.query('SELECT processor_name FROM processors WHERE id = $1', [processorId]);
  if (result.rowCount === 0) {
    throw new Error(`Processor with id ${processorId} not found`);
  }
  return result.rows[0].processor_name;
};

const applyTransformations = async (message, steps, dlqSteps) => {
  let transformedMessage = { ...message };

  for (let i = 0; i < steps.length; i++) {
    const processorName = await getProcessorName(steps[i]);
    const transformation = require(`../../stream-processor/transformations/${processorName}`);
    
    try {
      transformedMessage = transformation(transformedMessage);
      console.log('Ran process ' + processorName);
      console.log(transformedMessage);

      if (!transformedMessage) {
        if (dlqSteps && dlqSteps[i]) {
          const dlqTopicName = await getDlqTopicName(dlqSteps[i]);
          return { dlqMessage: message, dlqTopicName };
        } else {
          return { transformedMessage: null, filteredAt: processorName };
        }
      }
    } catch (error) {
      console.error(`Error applying transformation ${processorName}:`, error);
      return { transformedMessage: null, filteredAt: processorName, error: error.message };
    }
  }

  return { transformedMessage };
};

module.exports = {
  applyTransformations,
  getProcessorName,
};