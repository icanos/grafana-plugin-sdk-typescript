import { DataFrame } from "@grafana/data";
import { Table } from "apache-arrow";
export declare function marshalArrow(frame: DataFrame): Table;
