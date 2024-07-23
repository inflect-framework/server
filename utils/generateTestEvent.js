const avro = require('avro-js');
const protobuf = require('protobufjs');
const jsf = require('json-schema-faker');

function generateTestEvent(format, schema, protoName) {
  switch (format) {
    case 'avro':
      return generateAvroEvent(schema);
    case 'protobuf':
      return generateProtobufEvent(schema, protoName);
    case 'json':
      return generateJsonEvent(schema);
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

async function generateProtobufEvent(schema, protoName) {
  const root = await protobuf.load(schema);
  const messageType = root.lookupType(protoName);
  const avroSchema = protobufToAvroSchema(root, messageType);
  const avroType = avro.parse(avroSchema);
  return avroType.random();
}

function generateAvroEvent(schema) {
  const avroSchema = avro.parse(schema);
  console.log(avroSchema);
  return avroSchema.random();
}

async function generateJsonEvent(schema) {
  const value = await jsf.resolve(schema);
  return value;
}

module.exports = generateTestEvent;
