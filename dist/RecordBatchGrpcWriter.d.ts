import { RecordBatch, RecordBatchFileWriter, RecordBatchWriter, Schema, Table, TypeMap } from "apache-arrow";
export declare class RecordBatchGrpcWriter<T extends TypeMap = any> extends RecordBatchWriter<T> {
    static writeAll<T extends TypeMap = any>(input: Table<T> | Iterable<RecordBatch<T>>): RecordBatchFileWriter<T>;
    static writeAll<T extends TypeMap = any>(input: AsyncIterable<RecordBatch<T>>): Promise<RecordBatchFileWriter<T>>;
    static writeAll<T extends TypeMap = any>(input: PromiseLike<AsyncIterable<RecordBatch<T>>>): Promise<RecordBatchFileWriter<T>>;
    static writeAll<T extends TypeMap = any>(input: PromiseLike<Table<T> | Iterable<RecordBatch<T>>>): Promise<RecordBatchFileWriter<T>>;
    constructor();
    protected _writeSchema(schema: Schema<T>): this;
    protected _writeFooter(schema: Schema<T>): this;
}
