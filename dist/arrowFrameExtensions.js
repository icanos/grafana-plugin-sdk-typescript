"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.grafanaDataFrameToArrowTable = exports.arrowTableToDataFrame = exports.base64StringToArrowTable = void 0;
const apache_arrow_1 = require("apache-arrow");
const data_1 = require("@grafana/data");
function base64StringToArrowTable(text) {
    const b64 = atob(text);
    const arr = Uint8Array.from(b64, c => {
        return c.charCodeAt(0);
    });
    return apache_arrow_1.Table.from(arr);
}
exports.base64StringToArrowTable = base64StringToArrowTable;
function valueOrUndefined(val) {
    return val ? val : undefined;
}
function parseOptionalMeta(str) {
    if (str && str.length && str !== '{}') {
        try {
            return JSON.parse(str);
        }
        catch (err) {
            console.warn('Error reading JSON from arrow metadata: ', str);
        }
    }
    return undefined;
}
function vectorToArray(v) {
    const arr = Array(v.length);
    for (let i = 0; i < v.length; i++) {
        arr[i] = v.get(i);
    }
    return arr;
}
class FunctionalVector {
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
    forEach(iterator) {
        return data_1.vectorator(this).forEach(iterator);
    }
    map(transform) {
        return data_1.vectorator(this).map(transform);
    }
    filter(predicate) {
        return data_1.vectorator(this).filter(predicate);
    }
    toArray() {
        return vectorToArray(this);
    }
    toJSON() {
        return this.toArray();
    }
}
function arrowTableToDataFrame(table) {
    const fields = [];
    for (let i = 0; i < table.numCols; i++) {
        const col = table.getColumnAt(i);
        if (col) {
            const schema = table.schema.fields[i];
            let type = data_1.FieldType.other;
            let values = col;
            switch (schema.typeId) {
                case apache_arrow_1.ArrowType.Decimal:
                case apache_arrow_1.ArrowType.FloatingPoint: {
                    type = data_1.FieldType.number;
                    break;
                }
                case apache_arrow_1.ArrowType.Int: {
                    type = data_1.FieldType.number;
                    values = new NumberColumn(col); // Cast to number
                    break;
                }
                case apache_arrow_1.ArrowType.Bool: {
                    type = data_1.FieldType.boolean;
                    break;
                }
                case apache_arrow_1.ArrowType.Timestamp: {
                    type = data_1.FieldType.time;
                    break;
                }
                case apache_arrow_1.ArrowType.Utf8: {
                    type = data_1.FieldType.string;
                    break;
                }
                default:
                    console.log('UNKNOWN Type:', schema);
            }
            fields.push({
                name: col.name,
                type,
                values,
                config: parseOptionalMeta(col.metadata.get('config')) || {},
                labels: parseOptionalMeta(col.metadata.get('labels')),
            });
        }
    }
    const meta = table.schema.metadata;
    return {
        fields,
        length: table.length,
        refId: valueOrUndefined(meta.get('refId')),
        name: valueOrUndefined(meta.get('name')),
        meta: parseOptionalMeta(meta.get('meta')),
        table,
    };
}
exports.arrowTableToDataFrame = arrowTableToDataFrame;
function toArrowVector(field) {
    // OR: Float64Vector.from([1, 2, 3]));
    let type;
    if (field.type === data_1.FieldType.number) {
        type = new apache_arrow_1.Float64();
    }
    else if (field.type === data_1.FieldType.time) {
        type = new apache_arrow_1.TimestampMillisecond();
    }
    else if (field.type === data_1.FieldType.boolean) {
        type = new apache_arrow_1.Bool();
    }
    else if (field.type === data_1.FieldType.string) {
        type = new apache_arrow_1.Utf8();
    }
    else {
        type = new apache_arrow_1.Utf8();
    }
    const builder = apache_arrow_1.Builder.new({ type, nullValues: [null] });
    field.values.toArray().forEach(builder.append.bind(builder));
    return builder.finish().toVector();
}
/**
 * @param keepOriginalNames by default, the exported Table will get names that match the
 * display within grafana.  This typically includes any labels defined in the metadata.
 *
 * When using this function to round-trip data, be sure to set `keepOriginalNames=true`
 */
function grafanaDataFrameToArrowTable(data, keepOriginalNames) {
    // Return the original table
    let table = data.table;
    if (table instanceof apache_arrow_1.Table && table.numCols === data.fields.length) {
        if (!keepOriginalNames) {
            table = updateArrowTableNames(table, data);
        }
        if (table) {
            return table;
        }
    }
    table = apache_arrow_1.Table.new(data.fields.map((field, index) => {
        let name = field.name;
        // when used directly as an arrow table the name should match the arrow schema
        if (!keepOriginalNames) {
            name = data_1.getFieldDisplayName(field, data);
        }
        const column = apache_arrow_1.Column.new(name, toArrowVector(field));
        if (field.labels) {
            column.metadata.set('labels', JSON.stringify(field.labels));
        }
        if (field.config) {
            column.metadata.set('config', JSON.stringify(field.config));
        }
        return column;
    }));
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
exports.grafanaDataFrameToArrowTable = grafanaDataFrameToArrowTable;
function updateArrowTableNames(table, frame) {
    const cols = [];
    for (let i = 0; i < table.numCols; i++) {
        const col = table.getColumnAt(i);
        if (!col) {
            return undefined;
        }
        const name = data_1.getFieldDisplayName(frame.fields[i], frame);
        cols.push(apache_arrow_1.Column.new(col.field.clone({ name: name }), ...col.chunks));
    }
    return apache_arrow_1.Table.new(cols);
}
class NumberColumn extends FunctionalVector {
    constructor(col) {
        super();
        this.col = col;
    }
    get length() {
        return this.col.length;
    }
    get(index) {
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
}
//# sourceMappingURL=arrowFrameExtensions.js.map