import { DataFrame, Field as GrafanaField, FieldType } from "@grafana/data";
import { Bool, DataType, Field, Schema, Table, Utf8, Float64, Vector, Float64Builder, TimestampMicrosecond, TimestampMicrosecondBuilder, Utf8Builder } from "apache-arrow";
import { logger } from ".";

function makeTimestampVector(data: number[]): Vector<TimestampMicrosecond> {
  const builder = new TimestampMicrosecondBuilder({
    type: new TimestampMicrosecond(),
    nullValues: [null, undefined]
  });
  data.forEach((val) => builder.append(Number(val)));

  return builder.finish().toVector();
}

function makeFloatVector(data: number[]): Vector<Float64> {
  const builder = new Float64Builder({
    type: new Float64(),
    nullValues: [null, undefined]
  });
  data.forEach((val) => builder.append(Number(val)));

  return builder.finish().toVector();
}

function makeStringVector(data: string[]): Vector<Utf8> {
  const builder = new Utf8Builder({
    type: new Utf8(),
    nullValues: [null, undefined]
  });
  data.forEach((val) => builder.append(String(val)));

  return builder.finish().toVector();
}

export function marshalArrow(frame: DataFrame): Table {
  //if (frames && frames.length) {
  //  const firstFrame = frames[0];
  //  const grafanaFields = [...new Map(frames.flatMap((frame) => frame.fields.map(field => [field.name, field]))).values()];
  //  logger.info("grafanaFields", grafanaFields);
  const arrowFields = buildArrowFields(frame);
  const schema = buildArrowSchema(frame, arrowFields);
  const columns = buildArrowColumns(frame, arrowFields, schema);

  const table = new Table(...columns); //Table(schema, columns);
  logger.info("table", table);

  return table;
  //}

  //return undefined;
}

function buildArrowSchema(frame: DataFrame, fields: Field[]): Schema {
  const tableMetaMap: Map<string, string> = new Map<string, string>();
  tableMetaMap.set("name", frame.name ?? "");
  tableMetaMap.set("refId", frame.refId ?? "");

  if (frame.meta) {
    tableMetaMap.set("meta", JSON.stringify(frame.meta));
  }

  return new Schema(fields, tableMetaMap);
}

function buildArrowFields(frame: DataFrame): Field[] {
  const arrowFields = frame.fields.map((field) => {
    const {dataType: t, nullable} = fieldToArrow(field);

    const fieldMeta: Map<string, string> = new Map<string, string>();
    fieldMeta.set("name", field.name);

    if (field.labels) {
      fieldMeta.set("labels", JSON.stringify(field.labels));
    }
    if (field.config) {
      fieldMeta.set("config", JSON.stringify(field.config));
    }

    return new Field(field.name, t, nullable, fieldMeta);
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

function buildArrowColumns(frame: DataFrame, fields: Field[], schema: Schema): { [x: string]: Vector<any> }[] {
  const result: { [x: string]: Vector<any> }[] = [];
  let data: { [x: string]: Vector<any> } = {};

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

      data = {
        ...data,
        [frameField.name!]: values
      };
    }
  });

  result.push(data);

  return result;
}

function fieldToArrow(field: GrafanaField): {dataType: DataType, nullable: boolean; error: string | undefined} {
  switch (field.type) {
    case FieldType.boolean:
      return {
        dataType: new Bool(),
        nullable: false,
        error: undefined
      };
    case FieldType.number:
      return {
        dataType: new Float64(),
        nullable: false,
        error: undefined
      };
    case FieldType.time:
      return {
        dataType: new TimestampMicrosecond(),
        nullable: false,
        error: undefined
      };
    default:
      return {
        dataType: new Utf8(),
        nullable: false,
        error: undefined
      };
  }
}