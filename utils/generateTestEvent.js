const avro = require('avro-js');
const protobuf = require('protobufjs');
const jsf = require('json-schema-faker');
const fs = require('fs');
const path = require('path');

const sampleAvroSchema = JSON.parse(
  fs.readFileSync(path.resolve(__dirname, '../schemas/testAvro.avro'), 'utf8')
);
const sampleProtobufSchemaPath = path.resolve(
  __dirname,
  '../schemas/testProtobuf.proto'
);
const sampleJsonSchema = JSON.parse(
  fs.readFileSync(path.resolve(__dirname, '../schemas/testJSON.json'), 'utf8')
);

function generateTestEvent(format) {
  switch (format) {
    case 'avro':
      return generateAvroEvent();
    case 'protobuf':
      return generateProtobufEvent();
    case 'json':
      return generateJsonEvent();
    default:
      throw new Error('Invalid format');
  }
}

function protobufToAvroSchema(root, messageType) {
  const avroSchema = {
    type: 'record',
    name: messageType.name,
    fields: [],
  };

  messageType.fieldsArray.forEach((field) => {
    let fieldType;

    switch (field.type) {
      case 'int32':
      case 'int64':
      case 'uint32':
      case 'uint64':
        fieldType = 'int';
        break;
      case 'float':
      case 'double':
        fieldType = 'double';
        break;
      case 'string':
        fieldType = 'string';
        break;
      case 'bool':
        fieldType = 'boolean';
        break;
      case 'enum':
        fieldType = {
          type: 'enum',
          symbols: Object.values(field.resolvedType.values),
        };
        break;
      case 'message':
        fieldType = protobufToAvroSchema(root, field.resolvedType);
        break;
      case 'repeated':
        fieldType = {
          type: 'array',
          items: protobufToAvroSchema(root, field.resolvedType),
        };
        break;
      default:
        fieldType = 'null';
    }

    avroSchema.fields.push({ name: field.name, type: fieldType });
  });

  return avroSchema;
}

async function generateProtobufEvent(schemaPath = sampleProtobufSchemaPath) {
  const root = await protobuf.load(schemaPath);
  const messageType = root.lookupType('com.example.User'); // Adjust this to the correct path
  const avroSchema = protobufToAvroSchema(root, messageType);
  const avroType = avro.parse(avroSchema);
  return avroType.random();
}

function generateAvroEvent(schema = sampleAvroSchema) {
  const avroSchema = avro.parse(schema);
  return avroSchema.random();
}

async function generateJsonEvent(schema = sampleJsonSchema) {
  const value = await jsf.resolve(schema);
  return value;
}

// (async () => {
//   console.log(await generateTestEvent('avro'));
//   console.log(await generateTestEvent('protobuf'));
//   console.log(await generateTestEvent('json'));
// })();

module.exports = generateTestEvent;
