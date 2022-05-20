"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.marshalArrow = void 0;
const data_1 = require("@grafana/data");
const apache_arrow_1 = require("apache-arrow");
const _1 = require(".");
function marshalArrow(frame) {
    const arrowFields = buildArrowFields(frame);
    const schema = buildArrowSchema(frame, arrowFields);
    const columns = buildArrowColumns(frame, arrowFields, schema);
    const table = new apache_arrow_1.Table(...columns);
    _1.logger.info("table", table);
    return table;
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
function buildArrowColumns(frame, fields, schema) {
    const result = [];
    let data = {};
    fields.forEach((field) => {
        const frameField = frame.fields.find(x => x.name === field.name);
        if (frameField) {
            const builder = apache_arrow_1.makeBuilder({ type: field.type, nullValues: [null] });
            let rawValues = frameField.values;
            if (frameField.type === data_1.FieldType.time) {
                rawValues = new data_1.ArrayVector(rawValues.toArray().map((value) => Math.round(value.valueOf() / 1000)));
            }
            rawValues.toArray().forEach((val) => builder.append(getValue(frameField.type, val)));
            const values = builder.finish().toVector();
            _1.logger.info(frameField.name, "value length", values.length);
            _1.logger.info("values", values);
            data = Object.assign(Object.assign({}, data), { [frameField.name]: apache_arrow_1.makeData({ type: field.type, data: values }) });
        }
    });
    result.push(data);
    _1.logger.info("buildArrowColumns", result);
    return result;
}
function getValue(type, val) {
    switch (type) {
        case data_1.FieldType.number:
            return Number(val);
        case data_1.FieldType.boolean:
            return Boolean(val);
        case data_1.FieldType.time:
            return Number(val);
        default:
            return String(val);
    }
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
                dataType: new apache_arrow_1.TimestampMillisecond(),
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
/*import {
  Table,
  Builder,
  Vector as ArrowVector,
  Type as ArrowType,
  Float64,
  DataType,
  Utf8,
  TimestampMillisecond,
  Bool,
  makeVector,
  Schema,
  makeTable,
  Field as ArrowField,
  makeBuilder,
  List,
  vectorFromArray,
} from 'apache-arrow';
import { DataFrame, Field, FieldType, Vector, getFieldDisplayName, vectorator } from '@grafana/data';

export interface ArrowDataFrame extends DataFrame {
  table: Table;
}

function valueOrUndefined(val?: string) {
  return val ? val : undefined;
}

function parseOptionalMeta(str?: string): any {
  if (str && str.length && str !== '{}') {
    try {
      return JSON.parse(str);
    } catch (err) {
      console.warn('Error reading JSON from arrow metadata: ', str);
    }
  }
  return undefined;
}

function vectorToArray(v: any) {
  const arr = Array(v.length);
  for (let i = 0; i < v.length; i++) {
      arr[i] = v.get(i);
  }
  return arr;
}

abstract class FunctionalVector<T> implements Vector<T>, Iterable<T> {
  abstract length: number;
  abstract get(index: number): T;

  // Implement "iterator protocol"
  *iterator() {
      for (let i = 0; i < this.length; i++) {
          yield this.get(i);
      }
  }
  // Implement "iterable protocol"
  [Symbol.iterator]() {
      return this.iterator();
  }
  forEach(iterator: any) {
      return vectorator(this).forEach(iterator);
  }
  map(transform: any) {
      return vectorator(this).map(transform);
  }
  filter(predicate: any) {
      return vectorator(this).filter(predicate);
  }
  toArray() {
      return vectorToArray(this);
  }
  toJSON() {
      return this.toArray();
  }
}

export function arrowTableToDataFrame(table: Table): ArrowDataFrame {
  const fields: Field[] = [];

  for (let i = 0; i < table.numCols; i++) {
    const col = table.getChildAt(i);
    if (col) {
      const schema = table.schema.fields[i];
      let type = FieldType.other;
      let values: Vector<any> = col;
      switch ((schema.typeId as unknown) as ArrowType) {
        case ArrowType.Decimal:
        case ArrowType.Float: {
          type = FieldType.number;
          break;
        }
        case ArrowType.Int: {
          type = FieldType.number;
          values = new NumberColumn(col); // Cast to number
          break;
        }
        case ArrowType.Bool: {
          type = FieldType.boolean;
          break;
        }
        case ArrowType.Timestamp: {
          type = FieldType.time;
          break;
        }
        case ArrowType.Utf8: {
          type = FieldType.string;
          break;
        }
        default:
          console.log('UNKNOWN Type:', schema);
      }

      fields.push({
        name: col.VectorName,
        type,
        values,
        config: parseOptionalMeta(col["metadata"].get('config')) || {},
        labels: parseOptionalMeta(col["metadata"].get('labels')),
      });
    }
  }
  const meta = table.schema.metadata;
  return {
    fields,
    length: table.numRows,
    refId: valueOrUndefined(meta.get('refId')),
    name: valueOrUndefined(meta.get('name')),
    meta: parseOptionalMeta(meta.get('meta')),
    table,
  };
}

function getDataType(field: Field): DataType<ArrowType, any> {
  // OR: Float64Vector.from([1, 2, 3]));
  let type: DataType;
  if (field.type === FieldType.number) {
    type = new Float64();
  } else if (field.type === FieldType.time) {
    type = new TimestampMillisecond();
  } else if (field.type === FieldType.boolean) {
    type = new Bool();
  } else if (field.type === FieldType.string) {
    type = new Utf8();
  } else {
    type = new Utf8();
  }

  //return makeVector(field.values.toArray());
  return type;
}*/
/**
 * @param keepOriginalNames by default, the exported Table will get names that match the
 * display within grafana.  This typically includes any labels defined in the metadata.
 *
 * When using this function to round-trip data, be sure to set `keepOriginalNames=true`
 */
/*export function grafanaDataFrameToArrowTable(data: DataFrame, keepOriginalNames?: boolean): Table {
  // Return the original table
  let table = (data as any).table;
  if (table instanceof Table && table.numCols === data.fields.length) {
    if (!keepOriginalNames) {
      table = updateArrowTableNames(table, data);
    }
    if (table) {
      return table as Table;
    }
  }

  vectorFromArray()

  const cols = data.fields.map((field, index) => {
    let name = field.name;
    // when used directly as an arrow table the name should match the arrow schema
    if (!keepOriginalNames) {
      name = getFieldDisplayName(field, data);
    }
    const column = new ArrowField(name, getDataType(field), false, new Map<string, string>()); //Column.new(name, toArrowVector(field));
    if (field.labels) {
      column.metadata.set('labels', JSON.stringify(field.labels));
    }
    if (field.config) {
      column.metadata.set('config', JSON.stringify(field.config));
    }
    return column;
  });

  table = new Table(
    cols
  );
  const metadata = table.schema.metadata;
  if (data.name) {
    metadata.set('name', data.name);
  }
  if (data.refId) {
    metadata.set('refId', data.refId);
  }
  if (data.meta) {
    metadata.set('meta', JSON.stringify(data.meta));
  }
  return table;
}

function updateArrowTableNames(table: Table, frame: DataFrame): Table | undefined {
  const cols: Column[] = [];
  for (let i = 0; i < table.numCols; i++) {
    const col = table.getColumnAt(i);
    if (!col) {
      return undefined;
    }
    const name = getFieldDisplayName(frame.fields[i], frame);
    cols.push(Column.new(col.field.clone({ name: name }), ...col.chunks));
  }
  return Table.new(cols);
}

class NumberColumn extends FunctionalVector<number> {
  constructor(private col: Column) {
    super();
  }

  get length() {
    return this.col.length;
  }

  get(index: number): number {
    const v = this.col.get(index);
    if (v === null || isNaN(v)) {
      return v;
    }

    // The conversion operations are always silent, never give errors,
    // but if the bigint is too huge and won’t fit the number type,
    // then extra bits will be cut off, so we should be careful doing such conversion.
    // See https://javascript.info/bigint
    return Number(v);
  }
}*/ 
//# sourceMappingURL=arrowFrameExtensions.js.map