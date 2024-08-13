const applyProcessors = async (message, steps, dlqSteps) => {
  let transformedMessage = { ...message };
  let processingSteps = [];

  for (let i = 0; i < steps.length; i++) {
    const processorName = steps[i];
    const transformation = require(`../../stream-processor/src/transformations/${processorName}`);

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
