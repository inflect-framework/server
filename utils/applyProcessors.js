const applyProcessors = async (message, steps, dlqSteps) => {
  let transformedMessage = { ...message };
  let processingSteps = [];

  for (let i = 0; i < steps.length; i++) {
    const processorName = steps[i];
    let transformation;
    try {
      transformation = require(`../../stream-processor/src/transformations/${processorName}`);
    } catch (error) {
      if (error.code === 'MODULE_NOT_FOUND') {
        transformation = require(`../../stream-processor/src/filters/${processorName}`);
      } else {
        throw error;
      }
    }

    try {
      const prevMessage = JSON.parse(JSON.stringify(transformedMessage));
      transformedMessage = transformation(transformedMessage);
      
      processingSteps.push({
        name: processorName,
        status: 'success',
        input: prevMessage,
        output: transformedMessage
      });

      if (!transformedMessage) {
        if (dlqSteps && dlqSteps[i]) {
          return { 
            transformedMessage: null, 
            processingSteps,
            filteredAt: processorName,
            dlqTopic: dlqSteps[i]
          };
        } else {
          return { transformedMessage: null, processingSteps, filteredAt: processorName };
        }
      }
    } catch (error) {
      console.error(`Error applying transformation ${processorName}:`, error);
      processingSteps.push({
        name: processorName,
        status: 'error',
        error: error.message
      });
      return {
        transformedMessage: null,
        processingSteps,
        filteredAt: processorName,
        error: error.message,
      };
    }
  }

  return { transformedMessage, processingSteps };
};

module.exports = applyProcessors;
