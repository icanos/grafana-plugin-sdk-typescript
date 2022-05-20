"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.RecordBatchGrpcWriter = void 0;
const tslib_1 = require("tslib");
const apache_arrow_1 = require("apache-arrow");
const file_1 = require("apache-arrow/ipc/metadata/file");
const compat_1 = require("apache-arrow/src/util/compat");
class RecordBatchGrpcWriter extends apache_arrow_1.RecordBatchWriter {
    /** @nocollapse */
    static writeAll(input) {
        const writer = new apache_arrow_1.RecordBatchFileWriter();
        if (compat_1.isPromise(input)) {
            return input.then((x) => writer.writeAll(x));
        }
        else if (compat_1.isAsyncIterable(input)) {
            return writeAllAsync(writer, input);
        }
        return writeAll(writer, input);
    }
    constructor() {
        super();
        this._autoDestroy = false;
    }
    // @ts-ignore
    _writeSchema(schema) {
        return this._writeMagic()._writePadding(2);
    }
    _writeFooter(schema) {
        const buffer = file_1.Footer.encode(new file_1.Footer(schema, apache_arrow_1.MetadataVersion.V4, this._recordBatchBlocks, this._dictionaryBlocks));
        return super
            ._writeFooter(schema) // EOS bytes for sequential readers
            ._write(buffer) // Write the flatbuffer
            ._write(Int32Array.of(buffer.byteLength)) // then the footer size suffix
            ._writeMagic(); // then the magic suffix
    }
}
exports.RecordBatchGrpcWriter = RecordBatchGrpcWriter;
/** @ignore */
function writeAll(writer, input) {
    let chunks = input;
    if (input instanceof apache_arrow_1.Table) {
        chunks = input.batches;
        writer.reset(undefined, input.schema);
    }
    for (const batch of chunks) {
        writer.write(batch);
    }
    return writer.finish();
}
/** @ignore */
function writeAllAsync(writer, batches) {
    var batches_1, batches_1_1;
    var e_1, _a;
    return tslib_1.__awaiter(this, void 0, void 0, function* () {
        try {
            for (batches_1 = tslib_1.__asyncValues(batches); batches_1_1 = yield batches_1.next(), !batches_1_1.done;) {
                const batch = batches_1_1.value;
                writer.write(batch);
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (batches_1_1 && !batches_1_1.done && (_a = batches_1.return)) yield _a.call(batches_1);
            }
            finally { if (e_1) throw e_1.error; }
        }
        return writer.finish();
    });
}
//# sourceMappingURL=RecordBatchGrpcWriter.js.map