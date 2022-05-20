"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.marshalArrow = void 0;
const data_1 = require("@grafana/data");
const apache_arrow_1 = require("apache-arrow");
const _1 = require(".");
function makeTimestampVector(data) {
    const builder = new apache_arrow_1.TimestampMicrosecondBuilder({
        type: new apache_arrow_1.TimestampMicrosecond(),
        nullValues: [null, undefined]
    });
    data.forEach((val) => builder.append(Number(val)));
    return builder.finish().toVector();
}
function makeFloatVector(data) {
    const builder = new apache_arrow_1.Float64Builder({
        type: new apache_arrow_1.Float64(),
        nullValues: [null, undefined]
    });
    data.forEach((val) => builder.append(Number(val)));
    return builder.finish().toVector();
}
function makeStringVector(data) {
    const builder = new apache_arrow_1.Utf8Builder({
        type: new apache_arrow_1.Utf8(),
        nullValues: [null, undefined]
    });
    data.forEach((val) => builder.append(String(val)));
    return builder.finish().toVector();
}
function marshalArrow(frame) {
    //if (frames && frames.length) {
    //  const firstFrame = frames[0];
    //  const grafanaFields = [...new Map(frames.flatMap((frame) => frame.fields.map(field => [field.name, field]))).values()];
    //  logger.info("grafanaFields", grafanaFields);
    const arrowFields = buildArrowFields(frame);
    const schema = buildArrowSchema(frame, arrowFields);
    const columns = buildArrowColumns(frame, arrowFields, schema);
    const table = new apache_arrow_1.Table(...columns); //Table(schema, columns);
    _1.logger.info("table", table);
    return table;
    //}
    //return undefined;
}
exports.marshalArrow = marshalArrow;
function buildArrowSchema(frame, fields) {
    var _a, _b;
    const tableMetaMap = new Map();
    tableMetaMap.set("name", (_a = frame.name) !== null && _a !== void 0 ? _a : "");
    tableMetaMap.set("refId", (_b = frame.refId) !== null && _b !== void 0 ? _b : "");
    if (frame.meta) {
        tableMetaMap.set("meta", JSON.stringify(frame.meta));
    }
    return new apache_arrow_1.Schema(fields, tableMetaMap);
}
function buildArrowFields(frame) {
    const arrowFields = frame.fields.map((field) => {
        const { dataType: t, nullable } = fieldToArrow(field);
        const fieldMeta = new Map();
        fieldMeta.set("name", field.name);
        if (field.labels) {
            fieldMeta.set("labels", JSON.stringify(field.labels));
        }
        if (field.config) {
            fieldMeta.set("config", JSON.stringify(field.config));
        }
        return new apache_arrow_1.Field(field.name, t, nullable, fieldMeta);
    });
    return arrowFields;
}
/*function buildArrowColumns(frames: DataFrame[], fields: Field[], schema: Schema): RecordBatch[] {
  const result: { [x: string]: Vector<any> }[] = [];
  let data: { [x: string]: Vector<any> } = {};

  const test: RecordBatch[] = [];

  frames.forEach((frame) => {
    let frameData: { [x: string]: any } = {};
    logger.info("running frame", frame.name);

    fields.forEach((field) => {
      const frameField = frame.fields.find(x => x.name === field.name);

      if (frameField) {
        let values: Vector<any>;

        if (frameField.type === FieldType.time) {
          values = makeTimestampVector(frameField.values.toArray());
        }
        else if (frameField.type === FieldType.number) {
          values = makeFloatVector(frameField.values.toArray());
        }
        else {
          values = makeStringVector(frameField.values.toArray());
        }

        frameData = {
          ...frameData,
          [frameField.name!]: values
        };
      }
    });

    logger.info("ok, frameData is", frameData);
    const f = new RecordBatch(frameData);
    logger.info("ok2", f);
    test.push(new RecordBatch(schema, f.data));
  });

  result.push(data);

  return test;
}*/
function buildArrowColumns(frame, fields, schema) {
    const result = [];
    let data = {};
    fields.forEach((field) => {
        const frameField = frame.fields.find(x => x.name === field.name);
        if (frameField) {
            let values;
            if (frameField.type === data_1.FieldType.time) {
                values = makeTimestampVector(frameField.values.toArray());
            }
            else if (frameField.type === data_1.FieldType.number) {
                values = makeFloatVector(frameField.values.toArray());
            }
            else {
                values = makeStringVector(frameField.values.toArray());
            }
            data = Object.assign(Object.assign({}, data), { [frameField.name]: values });
        }
    });
    result.push(data);
    return result;
}
function fieldToArrow(field) {
    switch (field.type) {
        case data_1.FieldType.boolean:
            return {
                dataType: new apache_arrow_1.Bool(),
                nullable: false,
                error: undefined
            };
        case data_1.FieldType.number:
            return {
                dataType: new apache_arrow_1.Float64(),
                nullable: false,
                error: undefined
            };
        case data_1.FieldType.time:
            return {
                dataType: new apache_arrow_1.TimestampMicrosecond(),
                nullable: false,
                error: undefined
            };
        default:
            return {
                dataType: new apache_arrow_1.Utf8(),
                nullable: false,
                error: undefined
            };
    }
}
//# sourceMappingURL=arrow.js.map